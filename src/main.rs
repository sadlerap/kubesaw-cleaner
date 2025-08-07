use color_eyre::eyre::{eyre, Context};
use itertools::Itertools;
use k8s_openapi::{
    api::admissionregistration::v1::ValidatingWebhookConfiguration,
    apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition,
};
use kube::{
    api::{ApiResource, DynamicObject, GroupVersionKind, ListParams},
    core::Expression,
    Api, Client, Resource, ResourceExt,
};
use tracing::{error, info, instrument, level_filters::LevelFilter, trace};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;
    init_tracing()?;

    run().await?;
    Ok(())
}

fn init_tracing() -> color_eyre::Result<()> {
    let fmt_layer = fmt::layer().with_target(false);
    let filter_layer = EnvFilter::try_from_default_env().or_else(|_| {
        EnvFilter::builder()
            .with_default_directive(LevelFilter::INFO.into())
            .parse("")
    })?;

    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(filter_layer)
        .init();

    Ok(())
}

#[instrument]
async fn run() -> color_eyre::Result<()> {
    let client = Client::try_default().await?;

    // step 1: remove the "users.spacebindingsrequests.webhook.sandbox" validating webhook config,
    // since its existance blocks patching spacebindingrequests
    remove_webhook_configs(&client)
        .await
        .wrap_err("failed to remove stale webhooks, bailing")?;

    // step 2: get all kubesaw crds
    // they should have the OLM labels set, so we can use them
    let crds: Api<CustomResourceDefinition> = Api::all(client.clone());
    let host_expr = Expression::Exists(
        "operators.coreos.com/toolchain-host-operator.toolchain-host-operator".into(),
    );
    let host_lp = ListParams::default().labels_from(&host_expr.into());
    let host_crds = crds.list(&host_lp).await?;

    let member_expr = Expression::Exists(
        "operators.coreos.com/toolchain-member-operator.toolchain-member-operator".into(),
    );
    let member_lp = ListParams::default().labels_from(&member_expr.into());
    let member_crds = crds.list(&member_lp).await?;

    for crd in host_crds
        .iter()
        .chain(member_crds.iter())
        .unique_by(|crd| &crd.metadata.name)
    {
        info!(name = crd.metadata.name, "updating crds");
        // step 3: patch all instances of our custom resources to remove their finalizer
        let _ = patch_crs(&client, crd).await.inspect_err(|e| {
            error!(
                "Failed to update CRs for resource {:?}",
                crd.metadata.name.as_deref().unwrap_or("")
            );
            eprint!("{e}");
        });

        // step 4: remove the crd itself
        remove_crd(&client, crd)
            .await
            .wrap_err_with(|| format!("failed to delete crd {}", crd.name_any()))?;
    }

    Ok(())
}

async fn remove_webhook_configs(client: &Client) -> color_eyre::Result<()> {
    let webhooks: Api<ValidatingWebhookConfiguration> = Api::all(client.clone());
    webhooks
        .delete(
            "users.spacebindingrequests.webhook.sandbox",
            &Default::default(),
        )
        .await?
        .map_left(|_| info!("deleting webhooks"))
        .map_right(|_| info!("deleted webhook config"));

    Ok(())
}

#[instrument(skip_all, fields(crd_name = crd.name_any()))]
async fn remove_crd(client: &Client, crd: &CustomResourceDefinition) -> color_eyre::Result<()> {
    let crds: Api<CustomResourceDefinition> = Api::all(client.clone());
    if let Some(name) = crd.metadata.name.as_deref() {
        crds.delete(name, &Default::default())
            .await?
            .map_left(|_| info!("deleting crd"))
            .map_right(|_| info!("deleted crd"));
    }
    Ok(())
}

/// For a given custom resource definition, remove the finalizer for all instances
#[instrument(skip_all, fields(crd_name = crd.name_any()))]
async fn patch_crs(client: &Client, crd: &CustomResourceDefinition) -> color_eyre::Result<()> {
    let version = crd
        .spec
        .versions
        .iter()
        .find(|v| v.storage)
        .map(|v| v.name.clone())
        .ok_or_else(|| {
            error!(
                name = crd.meta().name,
                "failed to find storage version of crd"
            );
            color_eyre::eyre::eyre!("CRD parsing failed")
        })?;
    let dyntype = ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk(&crd.spec.group, &version, &crd.spec.names.kind),
        &crd.spec.names.plural,
    );
    trace!(
        version = dyntype.version,
        group = dyntype.group,
        kind = dyntype.kind,
        apiVersion = dyntype.api_version,
        plural = dyntype.plural
    );
    let cr_api: Api<DynamicObject> = Api::all_with(client.clone(), &dyntype);

    for (namespace, name) in cr_api
        .list(&ListParams::default())
        .await
        .map_err(|err| {
            error!(?err, "failed to retrieve custom resources");
            eyre!("failed to retrieve custom resources: {:?}", err)
        })?
        .iter()
        .filter_map(|cr| {
            Some((
                cr.metadata.namespace.as_deref()?,
                cr.metadata.name.as_deref()?,
            ))
        })
    {
        let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), namespace, &dyntype);
        let mut object = api.get(name).await.map_err(|err| {
            error!(?err, namespace, name, "failed to retrieve object");
            eyre!("failed to retrieve object: {:?}", err)
        })?;
        if let Some(finalizers) = object.metadata.finalizers {
            let new_finalizers = finalizers
                .iter()
                .filter(|f| *f != "finalizer.toolchain.dev.openshift.com")
                .cloned()
                .collect();

            object.metadata.finalizers = Some(new_finalizers);
            info!(name, namespace, "patching custom resource");
            let _ = api
                .replace(name, &Default::default(), &object)
                .await
                .inspect_err(|err| {
                    error!(?err, namespace, name, "failed to update finalizers");
                });
        }
    }

    Ok(())
}

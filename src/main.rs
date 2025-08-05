#![allow(dead_code)]
use color_eyre::eyre::eyre;
use itertools::Itertools;
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::{
    api::{ApiResource, DynamicObject, GroupVersionKind, ListParams},
    core::Expression,
    Api, Client, Resource,
};
use tracing::{error, info, instrument, level_filters::LevelFilter};
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

    // step 1: get all kubesaw crds
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
        let _ = patch_crs(&client, crd).await.inspect_err(|_| {
            error!("Failed to update CRs for resource {:?}", crd.metadata.name);
        });
    }

    Ok(())
}

/// For a given custom resource definition, remove the finalizer for all instances
#[instrument(skip_all, fields(name = crd.metadata.name))]
async fn patch_crs(client: &Client, crd: &CustomResourceDefinition) -> color_eyre::Result<()> {
    let version = crd
        .spec
        .versions
        .iter()
        .find(|v| v.served)
        .map(|v| v.name.clone())
        .ok_or_else(|| {
            error!(
                name = crd.meta().name,
                "failed to find served version of crd"
            );
            color_eyre::eyre::eyre!("CRD parsing failed")
        })?;
    let dyntype = ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk(&crd.spec.group, &version, &crd.spec.names.kind),
        &crd.spec.names.plural,
    );
    info!(
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
            api.replace(name, &Default::default(), &object)
                .await
                .map_err(|err| {
                    error!(?err, namespace, name, "failed to update finalizers");
                    eyre!("failed to update finalizers: {:?}", err)
                })?;
        }
    }

    Ok(())
}

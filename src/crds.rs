use color_eyre::eyre::{eyre, Context};
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::{
    api::{ApiResource, DynamicObject, GroupVersionKind, ListParams, ObjectList},
    core::Expression,
    Api, Client, Resource, ResourceExt,
};
use tracing::{error, info, instrument, trace};

pub async fn process_crd(
    client: &Client,
    crd: &CustomResourceDefinition,
) -> color_eyre::Result<()> {
    info!(name = crd.name_any(), "patching crd instances");
    let _ = patch_crs(&client, &crd).await.inspect_err(|e| {
        error!(
            "Failed to patch custom resource of type {:?}",
            crd.name_any()
        );
        eprint!("{e}")
    });

    remove_crd(&client, crd)
        .await
        .wrap_err_with(|| format!("failed to delete crd {}", crd.name_any()))?;
    Ok(())
}

/// fetches all [CustomResourceDefinition] instances with a given label.
pub async fn fetch_crds(
    client: &Client,
    label: &str,
) -> color_eyre::Result<ObjectList<CustomResourceDefinition>> {
    let crds: Api<CustomResourceDefinition> = Api::all(client.clone());

    let host_expr = Expression::Exists(label.to_owned());
    let host_lp = ListParams::default().labels_from(&host_expr.into());
    let host_crds = crds.list(&host_lp).await?;

    Ok(host_crds)
}

/// deletes a crd
#[instrument(skip_all, fields(crd_name = crd.name_any()))]
pub async fn remove_crd(client: &Client, crd: &CustomResourceDefinition) -> color_eyre::Result<()> {
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
pub async fn patch_crs(client: &Client, crd: &CustomResourceDefinition) -> color_eyre::Result<()> {
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

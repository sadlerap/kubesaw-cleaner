use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};
use color_eyre::eyre::Context;
use itertools::Itertools;
use k8s_openapi::api::admissionregistration::v1::ValidatingWebhookConfiguration;
use kube::{Api, Client};
use tracing::{info, instrument, level_filters::LevelFilter};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use crate::crds::{fetch_crds, process_crd};

mod crds;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct App {
    #[arg(short, long)]
    kubeconfig: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<Commands>,
}

static HOST_LABEL_KEY: &str =
    "operators.coreos.com/toolchain-host-operator.toolchain-host-operator";
static MEMBER_LABEL_KEY: &str =
    "operators.coreos.com/toolchain-member-operator.toolchain-member-operator";

#[derive(Subcommand)]
enum Commands {
    /// Removes kubesaw from a host cluster.
    Host,
    /// Removes kubesaw from a member cluster.
    Member(WebhookArgs),
    /// Removes kubesaw from a cluster with both host-operator and member-operator.
    All(WebhookArgs),
}

#[derive(Args)]
struct WebhookArgs {
    /// Attempt to remove known ValidatingWebhookConfigurations owned by kubesaw.
    #[arg(short, long)]
    webhook: bool,
}

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

async fn run() -> color_eyre::Result<()> {
    let app = App::parse();

    if let Some(kubeconfig) = app.kubeconfig {
        unsafe {
            std::env::set_var("KUBECONFIG", kubeconfig);
        }
    }

    let client = Client::try_default().await?;

    if let Some(command) = app.command {
        match command {
            Commands::Host => run_host(&client).await?,
            Commands::Member(args) => {
                if args.webhook {
                    remove_webhook_configs(&client)
                        .await
                        .wrap_err("failed to remove stale webhooks, bailing")?;
                }
                run_member(&client).await?;
            }
            Commands::All(args) => {
                if args.webhook {
                    remove_webhook_configs(&client)
                        .await
                        .wrap_err("failed to remove stale webhooks, bailing")?;
                }
                run_all(&client).await?
            },
        }
    }

    Ok(())
}

#[instrument(skip_all)]
async fn run_member(client: &Client) -> color_eyre::Result<()> {
    let member_crds = fetch_crds(client, MEMBER_LABEL_KEY).await?;
    for crd in member_crds {
        process_crd(client, &crd).await?;
    }

    Ok(())
}

#[instrument(skip_all)]
async fn run_host(client: &Client) -> color_eyre::Result<()> {
    let host_crds = fetch_crds(client, HOST_LABEL_KEY).await?;
    for crd in host_crds {
        process_crd(client, &crd).await?;
    }

    Ok(())
}

#[instrument(skip_all)]
async fn run_all(client: &Client) -> color_eyre::Result<()> {
    let host_crds = fetch_crds(client, HOST_LABEL_KEY).await?;
    let member_crds = fetch_crds(client, MEMBER_LABEL_KEY).await?;

    for crd in host_crds
        .iter()
        .chain(member_crds.iter())
        .unique_by(|crd| &crd.metadata.name)
    {
        process_crd(client, crd).await?;
    }

    Ok(())
}

async fn remove_webhook_configs(client: &Client) -> color_eyre::Result<()> {
    let webhooks: Api<ValidatingWebhookConfiguration> = Api::all(client.clone());
    webhooks
        .delete(
            "member-operator-validating-webhook-toolchain-member-operator",
            &Default::default(),
        )
        .await?
        .map_left(|_| info!("deleting webhooks"))
        .map_right(|_| info!("deleted webhook config"));

    Ok(())
}

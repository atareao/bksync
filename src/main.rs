pub mod cache;
pub mod cli;
pub mod config;
pub mod daemon;
pub mod error;
pub mod filter;
pub mod progress;
pub mod s3;
pub mod sync;

use std::path::PathBuf;

use clap::Parser;
use tracing::instrument;

use crate::cache::Cache;
use crate::cli::{Cli, Commands};
use crate::config::Config;
use crate::filter::Filter;
use crate::s3::S3Client;

fn setup_tracing(verbose: u8) {
    use tracing_subscriber::filter::EnvFilter;

    let level = match verbose {
        0 => "info",
        1 => "debug",
        _ => "trace",
    };

    let filter = EnvFilter::try_from_env("RUST_LOG")
        .unwrap_or_else(|_| EnvFilter::new(format!("bksync={}", level)));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .init();
}

fn merge_filters(config_patterns: &[String], cli_patterns: &[String]) -> Vec<String> {
    if cli_patterns.is_empty() {
        config_patterns.to_vec()
    } else {
        cli_patterns.to_vec()
    }
}

#[instrument(skip(cli))]
async fn run(cli: Cli) -> anyhow::Result<()> {
    setup_tracing(cli.verbose);

    let config = Config::load(&cli.config)?;
    let profile = config
        .profile
        .get(&cli.profile)
        .ok_or_else(|| anyhow::anyhow!("Profile '{}' not found in config", cli.profile))?;

    let concurrency = cli.concurrency.unwrap_or(profile.concurrency);

    tracing::info!("Using profile: {} (bucket: {}, endpoint: {})", cli.profile, profile.bucket, profile.endpoint);
    tracing::info!("Local directory: {}", profile.local_dir.display());
    tracing::info!("Concurrency: {}", concurrency);

    let s3 = S3Client::new(profile);
    let local_dir: PathBuf = profile.local_dir.clone();
    let mut cache = Cache::load(&cli.profile);

    let (include, exclude, delete, prefix) = match &cli.command {
        Commands::Pull { include, exclude, delete, path } => {
            (include.clone(), exclude.clone(), *delete, path.as_deref())
        }
        Commands::Push { include, exclude, delete, path } => {
            (include.clone(), exclude.clone(), *delete, path.as_deref())
        }
        Commands::Sync { include, exclude, delete, path } => {
            (include.clone(), exclude.clone(), *delete, path.as_deref())
        }
        Commands::Daemon { include, exclude, delete, path, .. } => {
            (include.clone(), exclude.clone(), *delete, path.as_deref())
        }
    };

    let include = merge_filters(&profile.include, &include);
    let exclude = merge_filters(&profile.exclude, &exclude);

    let filter = Filter::new(&include, &exclude)?;

    tracing::debug!("Include patterns: {:?}", include);
    tracing::debug!("Exclude patterns: {:?}", exclude);
    tracing::debug!("Dry-run: {}", cli.dry_run);
    tracing::debug!("Delete: {}", delete);

    let errors = match &cli.command {
        Commands::Pull { .. } => {
            sync::pull::pull(&s3, &local_dir, prefix, &filter, delete, cli.dry_run, concurrency, &mut cache, cli.summary).await?
        }
        Commands::Push { .. } => {
            sync::push::push(&s3, &local_dir, prefix, &filter, delete, cli.dry_run, concurrency, &mut cache, cli.summary).await?
        }
        Commands::Sync { .. } => {
            sync::bidirectional::bidirectional(&s3, &local_dir, prefix, &filter, delete, cli.dry_run, concurrency, &mut cache, cli.summary).await?
        }
        Commands::Daemon { refresh_minutes, debounce_ms, .. } => {
            daemon::run(
                &s3, &local_dir, prefix, &filter, delete, &mut cache,
                refresh_minutes * 60, *debounce_ms,
            ).await?
        }
    };

    cache.save();

    if !errors.is_empty() {
        tracing::error!("{} errors occurred during sync:", errors.len());
        for err in &errors {
            tracing::error!("  - {}", err);
        }
        Err(anyhow::anyhow!("Sync completed with {} errors", errors.len()))
    } else {
        tracing::info!("Sync completed successfully.");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    run(cli).await
}
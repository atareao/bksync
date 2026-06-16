use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use md5::{Digest, Md5};
use tokio::sync::Semaphore;
use tracing::instrument;

use crate::cache::Cache;
use crate::error::SyncError;
use crate::filter::Filter;
use crate::progress::Progress;
use crate::s3::S3Client;

pub fn md5_of_file(path: &Path) -> anyhow::Result<String> {
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Md5::new();
    let mut buffer = [0u8; 8192];
    loop {
        use std::io::Read;
        let bytes_read = file.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }
    let result = hasher.finalize();
    Ok(format!("{:x}", result))
}

pub fn normalize_etag(etag: &str) -> &str {
    etag.trim_matches('"')
}

pub fn is_multipart_etag(etag: &str) -> bool {
    etag.contains('-')
}

#[instrument]
pub fn checksums_match(local_path: &Path, s3_etag: &str) -> bool {
    let normalized = normalize_etag(s3_etag);
    if is_multipart_etag(normalized) {
        return false;
    }
    match md5_of_file(local_path) {
        Ok(local_md5) => local_md5 == normalized,
        Err(_) => false,
    }
}

pub fn find_parent_keys(objects: &[crate::s3::S3Object]) -> HashSet<String> {
    let all: HashSet<&str> = objects.iter().map(|o| o.key.as_str()).collect();
    objects
        .iter()
        .filter(|o| {
            let prefix = format!("{}/", o.key);
            all.iter().any(|k| k.starts_with(&prefix))
        })
        .map(|o| o.key.clone())
        .collect()
}

enum Action {
    Download { key: String, local_path: std::path::PathBuf, last_modified: DateTime<Utc> },
}

#[instrument(skip(s3, filter, cache))]
pub async fn pull(
    s3: &S3Client,
    local_dir: &Path,
    prefix: Option<&str>,
    filter: &Filter,
    delete: bool,
    dry_run: bool,
    concurrency: usize,
    cache: &mut Cache,
    summary: bool,
) -> anyhow::Result<Vec<SyncError>> {
    let progress = Arc::new(Progress::new());
    let errors = Arc::new(std::sync::Mutex::new(Vec::new()));
    let downloaded_keys: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(Vec::new()));

    let s3_objects = s3.list_objects(prefix).await?;
    let parent_keys = find_parent_keys(&s3_objects);
    progress.set_total(s3_objects.len() as u64);

    progress.start_operation("Pulling from S3", &format!("bucket -> {}", local_dir.display()));

    let mut seen_keys: HashMap<String, bool> = HashMap::new();
    let mut actions = Vec::new();

    for obj in &s3_objects {
        if parent_keys.contains(&obj.key) {
            tracing::warn!("Skipping '{}': also exists as directory prefix", obj.key);
            continue;
        }
        if !filter.matches(&obj.key) {
            tracing::debug!("Skipped (filtered): {}", obj.key);
            continue;
        }

        seen_keys.insert(obj.key.clone(), true);
        let local_path = local_dir.join(&obj.key);

        let needs_download = if local_path.exists() {
            let cached = cache.get(&obj.key);
            if let Some(entry) = cached {
                if entry.etag == obj.etag {
                    let cached_mtime_ns = entry.mtime;
                    let local_mtime = std::fs::metadata(&local_path)
                        .ok()
                        .and_then(|m| m.modified().ok())
                        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok().map(|d| d.as_nanos() as i64))
                        .unwrap_or(0);
                    if cached_mtime_ns == local_mtime {
                        progress.file_skipped(&obj.key);
                        continue;
                    }
                }
            }
            let s3_is_multipart = obj.etag.contains('-');
            if s3_is_multipart {
                let local_modified: DateTime<Utc> = std::fs::metadata(&local_path)
                    .ok()
                    .and_then(|m| m.modified().ok())
                    .map(|t| t.into())
                    .unwrap_or_default();
                obj.last_modified > local_modified
            } else {
                !checksums_match(&local_path, &obj.etag)
            }
        } else {
            true
        };

        if needs_download {
            if dry_run {
                progress.file_downloaded(&obj.key);
            } else {
                actions.push(Action::Download {
                    key: obj.key.clone(),
                    local_path,
                    last_modified: obj.last_modified,
                });
            }
        } else {
            if let Ok(md5) = md5_of_file(&local_path) {
                if let Ok(meta) = std::fs::metadata(&local_path) {
                    let mtime = meta
                        .modified()
                        .ok()
                        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok().map(|d| d.as_nanos() as i64))
                        .unwrap_or(0);
                    cache.set(
                        obj.key.clone(),
                        crate::cache::CacheEntry {
                            md5,
                            mtime,
                            etag: obj.etag.clone(),
                            last_modified: obj.last_modified.to_rfc3339(),
                        },
                    );
                }
            }
            progress.file_skipped(&obj.key);
        }
    }

    if !dry_run && !actions.is_empty() {
        let semaphore = Arc::new(Semaphore::new(concurrency));
        let s3 = s3.clone();
        let mut join_set = tokio::task::JoinSet::new();

        for action in actions {
            let permit = semaphore.clone().acquire_owned().await?;
            let s3 = s3.clone();
            let progress = progress.clone();
            let errors = errors.clone();
            let downloaded_keys = downloaded_keys.clone();

            join_set.spawn(async move {
                let _permit = permit;
                match action {
                    Action::Download { key, local_path, last_modified, .. } => {
                        let key_clone = key.clone();
                        tracing::info!("Downloading: {}", key);
                        if let Err(e) = s3.download_object(&key, &local_path).await {
                            errors.lock().unwrap().push(SyncError::new(&key, e.to_string()));
                            tracing::error!("Failed to download {}: {}", key, e);
                        } else {
                            let unix = last_modified.timestamp_nanos_opt().unwrap_or(0);
                            let secs = (unix / 1_000_000_000) as i64;
                            let nsecs = (unix % 1_000_000_000) as u32;
                            let dt = filetime::FileTime::from_unix_time(secs, nsecs);
                            if let Err(e) = filetime::set_file_mtime(&local_path, dt) {
                                tracing::error!("Failed to set mtime for {}: {}", key, e);
                            }
                            downloaded_keys.lock().unwrap().push(key_clone);
                            progress.file_downloaded(&key);
                        }
                    }
                }
            });
        }

        while let Some(result) = join_set.join_next().await {
            if let Err(e) = result {
                tracing::error!("Task panicked: {}", e);
            }
        }

        let keys = downloaded_keys.lock().unwrap();
        for key in keys.iter() {
            let local_path = local_dir.join(key);
            if let Ok(md5) = md5_of_file(&local_path) {
                if let Ok(meta) = std::fs::metadata(&local_path) {
                    let mtime = meta
                        .modified()
                        .ok()
                        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok().map(|d| d.as_nanos() as i64))
                        .unwrap_or(0);
                    if let Some(obj) = s3_objects.iter().find(|o| o.key == *key) {
                        cache.set(
                            key.clone(),
                            crate::cache::CacheEntry {
                                md5,
                                mtime,
                                etag: obj.etag.clone(),
                                last_modified: obj.last_modified.to_rfc3339(),
                            },
                        );
                    }
                }
            }
        }
    }

    if delete {
        let mut errs = errors.lock().unwrap();
        delete_local_orphans(local_dir, &seen_keys, filter, &progress, &mut errs, dry_run).await?;
    }

    let errs = Arc::into_inner(errors).unwrap().into_inner().unwrap();
    if summary {
        progress.summary("Pull complete");
    }
    Ok(errs)
}

async fn delete_local_orphans(
    local_dir: &Path,
    seen_keys: &HashMap<String, bool>,
    filter: &Filter,
    progress: &Progress,
    errors: &mut Vec<SyncError>,
    dry_run: bool,
) -> anyhow::Result<()> {
    for entry in walkdir::WalkDir::new(local_dir).into_iter().filter_map(|e| e.ok()) {
        if !entry.file_type().is_file() {
            continue;
        }
        let relative = entry
            .path()
            .strip_prefix(local_dir)
            .unwrap_or(entry.path());
        let key = relative.to_string_lossy().to_string();

        if !filter.matches(&key) {
            continue;
        }

        if !seen_keys.contains_key(&key) {
            if dry_run {
                progress.file_deleted(&key);
            } else {
                tracing::warn!("Deleting local file (not in S3): {}", key);
                if let Err(e) = std::fs::remove_file(entry.path()) {
                    errors.push(SyncError::new(&key, e.to_string()));
                    tracing::error!("Failed to delete {}: {}", key, e);
                } else {
                    progress.file_deleted(&key);
                }
            }
        }
    }
    Ok(())
}
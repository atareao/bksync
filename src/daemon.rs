use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use notify::{Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use tracing::instrument;

use crate::cache::{Cache, CacheEntry};
use crate::error::SyncError;
use crate::filter::Filter;
use crate::s3::S3Client;
use crate::sync::pull::md5_of_file;

#[instrument(skip(s3, filter, cache))]
pub async fn run(
    s3: &S3Client,
    local_dir: &Path,
    prefix: Option<&str>,
    filter: &Filter,
    delete: bool,
    cache: &mut Cache,
    refresh_secs: u64,
    debounce_ms: u64,
) -> anyhow::Result<Vec<SyncError>> {
    let errors: Arc<std::sync::Mutex<Vec<SyncError>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
    let debounce = Duration::from_millis(debounce_ms);

    tracing::info!("Performing initial S3 listing...");
    let s3_objects = s3.list_objects(prefix).await?;
    for obj in &s3_objects {
        let local_path = local_dir.join(&obj.key);
        if local_path.exists()
            && let Ok(md5) = md5_of_file(&local_path)
            && let Ok(meta) = std::fs::metadata(&local_path)
        {
            let mtime = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok().map(|d| d.as_nanos() as i64))
                .unwrap_or(0);
            cache.set(
                obj.key.clone(),
                CacheEntry {
                    md5,
                    mtime,
                    etag: obj.etag.clone(),
                    last_modified: obj.last_modified.to_rfc3339(),
                },
            );
        }
    }
    tracing::info!("Daemon ready. {} objects in cache.", s3_objects.len());

    let (notify_tx, notify_rx) = std::sync::mpsc::channel::<Result<Event, notify::Error>>();
    let mut watcher = RecommendedWatcher::new(notify_tx, Config::default())
        .map_err(|e| anyhow::anyhow!("Failed to create file watcher: {}", e))?;
    watcher
        .watch(local_dir, RecursiveMode::Recursive)
        .map_err(|e| anyhow::anyhow!("Failed to watch directory: {}", e))?;

    let (tx, mut rx) = tokio::sync::mpsc::channel::<Event>(256);
    let local_dir_clone = local_dir.to_path_buf();
    let filter_clone = filter.clone();
    std::thread::spawn(move || {
        for result in notify_rx {
            match result {
                Ok(event) => {
                    let relevant = matches!(
                        event.kind,
                        EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_)
                    );
                    if !relevant {
                        continue;
                    }
                    let mut filtered = false;
                    for path in &event.paths {
                        if !path.starts_with(&local_dir_clone) {
                            filtered = true;
                            break;
                        }
                        let rel = path.strip_prefix(&local_dir_clone).unwrap_or(path);
                        let key = rel.to_string_lossy();
                        if !filter_clone.matches(&key) {
                            filtered = true;
                            break;
                        }
                    }
                    if filtered {
                        continue;
                    }
                    if tx.blocking_send(event).is_err() {
                        break;
                    }
                }
                Err(e) => tracing::error!("Notify error: {}", e),
            }
        }
    });

    tracing::info!("Watching: {}", local_dir.display());

    let mut refresh_interval = tokio::time::interval(Duration::from_secs(refresh_secs));
    refresh_interval.tick().await;

    loop {
        let mut pending: HashMap<PathBuf, EventKind> = HashMap::new();

        tokio::select! {
            Some(event) = rx.recv() => {
                collect_events(&mut pending, event);
                tokio::time::sleep(debounce).await;
                while let Ok(event) = rx.try_recv() {
                    collect_events(&mut pending, event);
                }
            }
            _ = refresh_interval.tick() => {
                tracing::info!("Periodic S3 refresh...");
                if let Err(e) = refresh_from_s3(s3, local_dir, filter, delete, cache, &errors).await {
                    tracing::error!("S3 refresh failed: {}", e);
                }
                continue;
            }
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("Daemon shutting down.");
                break;
            }
        }

        if pending.is_empty() {
            continue;
        }

        tracing::debug!("Processing {} local change(s)", pending.len());
        for (path, kind) in &pending {
            let rel = path.strip_prefix(local_dir).unwrap_or(path);
            let key = rel.to_string_lossy().to_string();

            match kind {
                EventKind::Remove(_) => {
                    if delete {
                        tracing::info!("Local delete -> S3: {}", key);
                        if let Err(e) = s3.delete_object(&key).await {
                            errors.lock().unwrap().push(SyncError::new(&key, e.to_string()));
                            tracing::error!("Failed to delete {} from S3: {}", key, e);
                        } else {
                            cache.remove(&key);
                        }
                    }
                }
                _ => {
                    if let Err(e) = handle_local_change(s3, local_dir, &key, cache).await {
                        errors.lock().unwrap().push(SyncError::new(&key, e.to_string()));
                        tracing::error!("Failed to sync {}: {}", key, e);
                    }
                }
            }
        }
    }

    cache.save();
    let errs = Arc::into_inner(errors).unwrap().into_inner().unwrap();
    Ok(errs)
}

fn collect_events(
    pending: &mut HashMap<PathBuf, EventKind>,
    event: Event,
) {
    for path in event.paths {
        pending.insert(path, event.kind);
    }
}

async fn handle_local_change(
    s3: &S3Client,
    local_dir: &Path,
    key: &str,
    cache: &mut Cache,
) -> anyhow::Result<()> {
    let local_path = local_dir.join(key);

    if !local_path.exists() {
        return Ok(());
    }

    let local_meta = std::fs::metadata(&local_path)?;
    let local_modified = local_meta.modified()?;
    let local_mtime = local_modified
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0);

    if let Some(entry) = cache.get(key)
        && entry.mtime == local_mtime && !entry.md5.is_empty()
    {
        return Ok(());
    }

    match s3.head_object(key).await {
        Ok((s3_etag, s3_last_modified)) => {
            let s3_mtime = s3_last_modified.timestamp_nanos_opt().unwrap_or(0);
            let local_md5 = md5_of_file(&local_path).unwrap_or_default();

            if local_mtime > s3_mtime {
                tracing::info!("Upload (newer): {}", key);
                upload_file(s3, key, &local_path, &local_md5, local_mtime, cache).await?;
            } else if local_mtime < s3_mtime {
                tracing::warn!("Conflict: {} - S3 is newer, creating conflict file", key);
                let conflict_name = format!("{}.conflict-{}", key, local_mtime);
                let conflict_path = local_dir.join(&conflict_name);
                tokio::fs::rename(&local_path, &conflict_path).await?;
                tracing::info!("Created conflict file: {}", conflict_name);
                s3.download_object(key, &local_path).await?;
                if let Ok(md5) = md5_of_file(&local_path)
                    && let Ok(meta) = std::fs::metadata(&local_path)
                {
                    let mtime = meta
                        .modified()
                        .ok()
                        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok().map(|d| d.as_nanos() as i64))
                        .unwrap_or(0);
                    cache.set(
                        key.to_string(),
                        CacheEntry {
                            md5,
                            mtime,
                            etag: s3_etag,
                            last_modified: s3_last_modified.to_rfc3339(),
                        },
                    );
                }
            } else {
                if local_md5 != s3_etag {
                    tracing::warn!("Conflict: {} - same timestamp, different content", key);
                    let conflict_name = format!("{}.conflict-{}", key, local_mtime);
                    let conflict_path = local_dir.join(&conflict_name);
                    tokio::fs::rename(&local_path, &conflict_path).await?;
                    tracing::info!("Created conflict file: {}", conflict_name);
                    upload_file(s3, key, &local_path, &local_md5, local_mtime, cache).await?;
                } else {
                    tracing::debug!("Unchanged: {}", key);
                    cache.set(
                        key.to_string(),
                        CacheEntry {
                            md5: local_md5,
                            mtime: local_mtime,
                            etag: s3_etag,
                            last_modified: s3_last_modified.to_rfc3339(),
                        },
                    );
                }
            }
        }
        Err(_) => {
            tracing::info!("Upload (new): {}", key);
            let local_md5 = md5_of_file(&local_path).unwrap_or_default();
            upload_file(s3, key, &local_path, &local_md5, local_mtime, cache).await?;
        }
    }

    Ok(())
}

async fn upload_file(
    s3: &S3Client,
    key: &str,
    local_path: &Path,
    md5: &str,
    mtime: i64,
    cache: &mut Cache,
) -> anyhow::Result<()> {
    use std::collections::HashMap;

    use crate::s3::{CACHE_MD5, CACHE_MTIME};

    let mut meta = HashMap::new();
    meta.insert(CACHE_MD5.to_string(), md5.to_string());
    meta.insert(CACHE_MTIME.to_string(), mtime.to_string());

    s3.upload_object(key, local_path, Some(&meta)).await?;

    cache.set(
        key.to_string(),
        CacheEntry {
            md5: md5.to_string(),
            mtime,
            etag: String::new(),
            last_modified: chrono::Utc::now().to_rfc3339(),
        },
    );

    Ok(())
}

async fn refresh_from_s3(
    s3: &S3Client,
    local_dir: &Path,
    filter: &Filter,
    delete: bool,
    cache: &mut Cache,
    errors: &Arc<std::sync::Mutex<Vec<SyncError>>>,
) -> anyhow::Result<()> {
    let s3_objects = s3.list_objects(None).await?;

    for obj in &s3_objects {
        if !filter.matches(&obj.key) {
            continue;
        }

        let local_path = local_dir.join(&obj.key);

        if let Some(entry) = cache.get(&obj.key)
            && entry.etag == obj.etag
        {
            continue;
        }

        if !local_path.exists() {
            tracing::info!("Download (new from S3): {}", obj.key);
            if let Err(e) = s3.download_object(&obj.key, &local_path).await {
                errors.lock().unwrap().push(SyncError::new(&obj.key, e.to_string()));
                continue;
            }
        } else {
            let local_modified: chrono::DateTime<chrono::Utc> = std::fs::metadata(&local_path)
                .ok()
                .and_then(|m| m.modified().ok())
                .map(|t| t.into())
                .unwrap_or_default();

            if obj.last_modified > local_modified {
                if let Some(entry) = cache.get(&obj.key) {
                    let local_mtime = std::fs::metadata(&local_path)
                        .ok()
                        .and_then(|m| m.modified().ok())
                        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok().map(|d| d.as_nanos() as i64))
                        .unwrap_or(0);

                    if entry.mtime != local_mtime {
                        tracing::warn!("Conflict: {} - both sides modified", obj.key);
                        let conflict_name = format!("{}.conflict-{}", obj.key, local_mtime);
                        let conflict_path = local_dir.join(&conflict_name);
                        tokio::fs::rename(&local_path, &conflict_path).await?;
                        tracing::info!("Created conflict file: {}", conflict_name);
                    }
                }

                tracing::info!("Download (newer from S3): {}", obj.key);
                if let Err(e) = s3.download_object(&obj.key, &local_path).await {
                    errors.lock().unwrap().push(SyncError::new(&obj.key, e.to_string()));
                    continue;
                }
            } else {
                continue;
            }
        }

        if let Ok(md5) = md5_of_file(&local_path)
            && let Ok(meta) = std::fs::metadata(&local_path)
        {
            let mtime = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok().map(|d| d.as_nanos() as i64))
                .unwrap_or(0);
            cache.set(
                obj.key.clone(),
                CacheEntry {
                    md5,
                    mtime,
                    etag: obj.etag.clone(),
                    last_modified: obj.last_modified.to_rfc3339(),
                },
            );
        }
    }

    if delete {
        let s3_keys: std::collections::HashSet<String> =
            s3_objects.iter().map(|o| o.key.clone()).collect();
        for entry in walkdir::WalkDir::new(local_dir)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if !entry.file_type().is_file() {
                continue;
            }
            let relative = entry.path().strip_prefix(local_dir).unwrap_or(entry.path());
            let key = relative.to_string_lossy().to_string();

            if filter.matches(&key) && !s3_keys.contains(&key) {
                tracing::info!("Deleting local orphan: {}", key);
                if let Err(e) = std::fs::remove_file(entry.path()) {
                    errors.lock().unwrap().push(SyncError::new(&key, e.to_string()));
                } else {
                    cache.remove(&key);
                }
            }
        }
    }

    Ok(())
}
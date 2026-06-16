use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use tokio::sync::Semaphore;
use tracing::instrument;

use crate::cache::{Cache, CacheEntry};
use crate::error::SyncError;
use crate::filter::Filter;
use crate::progress::Progress;
use crate::s3::{S3Client, CACHE_MD5, CACHE_MTIME};
use crate::sync::pull::{checksums_match, find_parent_keys};

enum Action {
    Download { key: String, local_path: std::path::PathBuf, last_modified: DateTime<Utc> },
    Upload { key: String, local_path: std::path::PathBuf, md5: String, mtime: i64 },
    DeleteLocal { key: String, local_path: std::path::PathBuf },
}

#[instrument(skip(s3, filter, cache))]
pub async fn bidirectional(
    s3: &S3Client,
    local_dir: &Path,
    prefix: Option<&str>,
    filter: &Filter,
    delete: bool,
    dry_run: bool,
    concurrency: usize,
    cache: &mut Cache,
) -> anyhow::Result<Vec<SyncError>> {
    let progress = Arc::new(Progress::new());
    let errors = Arc::new(std::sync::Mutex::new(Vec::new()));

    progress.start_operation("Bidirectional sync", &format!("{} <-> bucket", local_dir.display()));

    let mut s3_map = s3.list_objects_map(prefix).await?;
    let parent_keys = find_parent_keys(
        &s3_map.values().cloned().collect::<Vec<_>>(),
    );
    s3_map.retain(|k, _| !parent_keys.contains(k));
    for key in &parent_keys {
        tracing::warn!("Skipping S3 key '{}': also exists as directory prefix", key);
    }

    let mut local_map: HashMap<String, (std::path::PathBuf, DateTime<Utc>)> = HashMap::new();
    for entry in walkdir::WalkDir::new(local_dir)
        .into_iter()
        .filter_map(|e| e.ok())
    {
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
        let modified: DateTime<Utc> = entry
            .metadata()
            .ok()
            .and_then(|m| m.modified().ok())
            .map(|t| t.into())
            .unwrap_or_default();
        local_map.insert(key.clone(), (entry.path().to_path_buf(), modified));
    }

    let all_keys: BTreeSet<String> = {
        let mut set = BTreeSet::new();
        for k in s3_map.keys() {
            if filter.matches(k) {
                set.insert(k.clone());
            }
        }
        for k in local_map.keys() {
            if filter.matches(k) {
                set.insert(k.clone());
            }
        }
        set
    };

    progress.set_total(all_keys.len() as u64);

    let mut synced_keys: HashMap<String, bool> = HashMap::new();
    let mut actions = Vec::new();
    let mut s3_delete_keys = Vec::new();

    for key in &all_keys {
        let in_s3 = s3_map.contains_key(key);
        let in_local = local_map.contains_key(key);

        synced_keys.insert(key.clone(), true);

        match (in_s3, in_local) {
            (true, false) => {
                tracing::debug!("sync ONLY_IN_S3: '{}'", key);
                let local_path = local_dir.join(key);
                if dry_run {
                    progress.file_downloaded(key);
                } else {
                    actions.push(Action::Download {
                        key: key.clone(),
                        local_path,
                        last_modified: s3_map[key].last_modified,
                    });
                }
            }
            (false, true) => {
                tracing::debug!("sync ONLY_LOCAL: '{}'", key);
                let (local_path, _) = &local_map[key];
                let local_mtime = std::fs::metadata(local_path)
                    .ok()
                    .and_then(|m| m.modified().ok())
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok().map(|d| d.as_nanos() as i64))
                    .unwrap_or(0);

                let cache_hit = cache.get(key).and_then(|entry| {
                    if entry.mtime == local_mtime && !entry.md5.is_empty() {
                        Some(())
                    } else {
                        None
                    }
                });

                if cache_hit.is_some() {
                    progress.file_skipped(key);
                    continue;
                }

                if dry_run {
                    progress.file_uploaded(key);
                } else {
                    let md5 = crate::sync::pull::md5_of_file(local_path).unwrap_or_default();
                    actions.push(Action::Upload {
                        key: key.clone(),
                        local_path: local_path.clone(),
                        md5,
                        mtime: local_mtime,
                    });
                }
            }
            (true, true) => {
                let s3_obj = &s3_map[key];
                let (local_path, local_modified) = &local_map[key];

                let diff = s3_obj.last_modified - *local_modified;
                tracing::debug!("sync decide '{}': s3_ts={:?}, local_ts={:?}, diff_secs={}", key, s3_obj.last_modified, local_modified, diff.num_seconds());

                if diff.num_seconds() > 0 {
                    if dry_run {
                        progress.file_downloaded(key);
                    } else {
                        actions.push(Action::Download {
                            key: key.clone(),
                            local_path: local_path.clone(),
                            last_modified: s3_obj.last_modified,
                        });
                    }
                } else if diff.num_seconds() < 0 {
                    let local_mtime = std::fs::metadata(local_path)
                        .ok()
                        .and_then(|m| m.modified().ok())
                        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok().map(|d| d.as_nanos() as i64))
                        .unwrap_or(0);

                    let cache_hit = cache.get(key).and_then(|entry| {
                        if entry.mtime == local_mtime && !entry.md5.is_empty() {
                            Some(())
                        } else {
                            None
                        }
                    });

                    if cache_hit.is_some() {
                        progress.file_skipped(key);
                        continue;
                    }

                    if checksums_match(local_path, &s3_obj.etag) {
                        let local_mtime = std::fs::metadata(local_path)
                            .ok()
                            .and_then(|m| m.modified().ok())
                            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok().map(|d| d.as_nanos() as i64))
                            .unwrap_or(0);
                        cache.set(
                            key.clone(),
                            crate::cache::CacheEntry {
                                md5: md5_of_file_fast(local_path).unwrap_or_default(),
                                mtime: local_mtime,
                                etag: s3_obj.etag.clone(),
                                last_modified: s3_obj.last_modified.to_rfc3339(),
                            },
                        );
                        progress.file_skipped(key);
                        continue;
                    }

                    if dry_run {
                        progress.file_uploaded(key);
                    } else {
                        let md5 = crate::sync::pull::md5_of_file(local_path).unwrap_or_default();
                        actions.push(Action::Upload {
                            key: key.clone(),
                            local_path: local_path.clone(),
                            md5,
                            mtime: local_mtime,
                        });
                    }
                } else {
                    if !checksums_match(local_path, &s3_obj.etag) {
                        if dry_run {
                            progress.file_uploaded(key);
                        } else {
                            tracing::warn!("Conflict (same timestamp, different content) for {}. Local wins.", key);
                            let local_mtime = std::fs::metadata(local_path)
                                .ok()
                                .and_then(|m| m.modified().ok())
                                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok().map(|d| d.as_nanos() as i64))
                                .unwrap_or(0);
                            let md5 = crate::sync::pull::md5_of_file(local_path).unwrap_or_default();
                            actions.push(Action::Upload {
                                key: key.clone(),
                                local_path: local_path.clone(),
                                md5,
                                mtime: local_mtime,
                            });
                        }
                    } else {
                        let local_mtime = std::fs::metadata(local_path)
                            .ok()
                            .and_then(|m| m.modified().ok())
                            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok().map(|d| d.as_nanos() as i64))
                            .unwrap_or(0);
                        cache.set(
                            key.clone(),
                            CacheEntry {
                                md5: md5_of_file_fast(local_path).unwrap_or_default(),
                                mtime: local_mtime,
                                etag: s3_obj.etag.clone(),
                                last_modified: s3_obj.last_modified.to_rfc3339(),
                            },
                        );
                        progress.file_skipped(key);
                    }
                }
            }
            _ => {}
        }
    }

    if delete {
        for (key, (local_path, _)) in &local_map {
            if !synced_keys.contains_key(key) {
                if dry_run {
                    progress.file_deleted(key);
                } else {
                    actions.push(Action::DeleteLocal {
                        key: key.clone(),
                        local_path: local_path.clone(),
                    });
                }
            }
        }

        for key in s3_map.keys() {
            if !synced_keys.contains_key(key) && filter.matches(key) {
                if dry_run {
                    progress.file_deleted(key);
                } else {
                    s3_delete_keys.push(key.clone());
                }
            }
        }
    }

    if !dry_run && !actions.is_empty() {
        let semaphore = Arc::new(Semaphore::new(concurrency));
        let s3 = s3.clone();
        let mut join_set = tokio::task::JoinSet::new();
        let completed_downloads = Arc::new(std::sync::Mutex::new(Vec::new()));
        let completed_uploads = Arc::new(std::sync::Mutex::new(Vec::new()));

        for action in actions {
            let permit = semaphore.clone().acquire_owned().await?;
            let s3 = s3.clone();
            let progress = progress.clone();
            let errors = errors.clone();
            let completed_downloads = completed_downloads.clone();
            let completed_uploads = completed_uploads.clone();

            join_set.spawn(async move {
                let _permit = permit;
                match action {
                    Action::Download { key, local_path, last_modified, .. } => {
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
                            completed_downloads.lock().unwrap().push(key.clone());
                            progress.file_downloaded(&key);
                        }
                    }
                    Action::Upload { key, local_path, md5, mtime } => {
                        tracing::info!("Uploading: {}", key);
                        let mut meta = HashMap::new();
                        meta.insert(CACHE_MD5.to_string(), md5.clone());
                        meta.insert(CACHE_MTIME.to_string(), mtime.to_string());
                        if let Err(e) = s3.upload_object(&key, &local_path, Some(&meta)).await {
                            errors.lock().unwrap().push(SyncError::new(&key, e.to_string()));
                            tracing::error!("Failed to upload {}: {}", key, e);
                        } else {
                            completed_uploads.lock().unwrap().push((key.clone(), md5, mtime));
                            progress.file_uploaded(&key);
                        }
                    }
                    Action::DeleteLocal { key, local_path } => {
                        tracing::warn!("Deleting local (not in S3): {}", key);
                        if let Err(e) = std::fs::remove_file(&local_path) {
                            errors.lock().unwrap().push(SyncError::new(&key, e.to_string()));
                        } else {
                            progress.file_deleted(&key);
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

        for key in Arc::into_inner(completed_downloads).unwrap().into_inner().unwrap() {
            let local_path = local_dir.join(&key);
            let md5 = crate::sync::pull::md5_of_file(&local_path).unwrap_or_default();
            let mtime = std::fs::metadata(&local_path)
                .ok()
                .and_then(|m| m.modified().ok())
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok().map(|d| d.as_nanos() as i64))
                .unwrap_or(0);
            if let Some(obj) = s3_map.get(&key) {
                cache.set(
                    key,
                    CacheEntry {
                        md5,
                        mtime,
                        etag: obj.etag.clone(),
                        last_modified: obj.last_modified.to_rfc3339(),
                    },
                );
            }
        }

        for (key, md5, mtime) in Arc::into_inner(completed_uploads).unwrap().into_inner().unwrap() {
            let etag = s3_map.get(&key).map(|o| o.etag.clone()).unwrap_or_default();
            let last_modified = s3_map.get(&key).map(|o| o.last_modified.to_rfc3339()).unwrap_or_default();
            cache.set(key, CacheEntry { md5, mtime, etag, last_modified });
        }
    }

    if !dry_run && !s3_delete_keys.is_empty() {
        if let Err(e) = s3.delete_objects(&s3_delete_keys).await {
            tracing::error!("Failed to batch delete S3 objects: {}", e);
        } else {
            for key in &s3_delete_keys {
                cache.remove(key);
                progress.file_deleted(key);
            }
        }
    }

    let errs = Arc::into_inner(errors).unwrap().into_inner().unwrap();
    progress.summary("Sync complete");
    Ok(errs)
}

fn md5_of_file_fast(path: &Path) -> anyhow::Result<String> {
    crate::sync::pull::md5_of_file(path)
}
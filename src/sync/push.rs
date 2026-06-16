use std::collections::HashMap;
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
use crate::sync::pull::md5_of_file;

struct FileAction {
    key: String,
    local_path: std::path::PathBuf,
    md5: String,
    mtime: i64,
}

#[instrument(skip(s3, filter, cache))]
pub async fn push(
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

    let s3_map = s3.list_objects_map(prefix).await?;

    let mut local_files: Vec<(String, std::path::PathBuf)> = Vec::new();
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
        local_files.push((key.clone(), entry.path().to_path_buf()));
    }

    progress.set_total(local_files.len() as u64);
    progress.start_operation("Pushing to S3", &format!("{} -> bucket", local_dir.display()));

    let mut seen_keys: HashMap<String, bool> = HashMap::new();
    let mut uploads = Vec::new();
    let mut s3_delete_keys = Vec::new();

    for (key, local_path) in &local_files {
        let effective_key = match prefix {
            Some(p) => format!("{}/{}", p.trim_end_matches('/'), key),
            None => key.clone(),
        };

        seen_keys.insert(effective_key.clone(), true);

        let local_mtime = std::fs::metadata(local_path)
            .ok()
            .and_then(|m| m.modified().ok())
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok().map(|d| d.as_nanos() as i64))
            .unwrap_or(0);

        let cache_hit = cache.get(&effective_key).and_then(|entry| {
            if entry.mtime == local_mtime && !entry.md5.is_empty() {
                Some(())
            } else {
                None
            }
        });

        if cache_hit.is_some() {
            progress.file_skipped(&effective_key);
            continue;
        }

        let needs_upload = match s3_map.get(&effective_key) {
            Some(obj) => {
                let local_md5 = md5_of_file(local_path).unwrap_or_default();
                let s3_etag = obj.etag.trim_matches('"');
                if is_multipart(s3_etag) {
                    let local_modified: DateTime<Utc> = std::fs::metadata(local_path)
                        .ok()
                        .and_then(|m| m.modified().ok())
                        .map(|t| t.into())
                        .unwrap_or_default();
                    local_modified > obj.last_modified
                } else {
                    local_md5 != s3_etag
                }
            }
            None => true,
        };

        if needs_upload {
            if dry_run {
                progress.file_uploaded(&effective_key);
            } else {
                let local_md5 = md5_of_file(local_path).unwrap_or_default();
                uploads.push(FileAction {
                    key: effective_key,
                    local_path: local_path.clone(),
                    md5: local_md5,
                    mtime: local_mtime,
                });
            }
        } else {
            let local_md5 = md5_of_file(local_path).unwrap_or_default();
            cache.set(
                effective_key.clone(),
                CacheEntry {
                    md5: local_md5,
                    mtime: local_mtime,
                    etag: s3_map[&effective_key].etag.clone(),
                    last_modified: s3_map[&effective_key].last_modified.to_rfc3339(),
                },
            );
            progress.file_skipped(&effective_key);
        }
    }

    if delete {
        for (key, _obj) in &s3_map {
            if !seen_keys.contains_key(key) && filter.matches(key) {
                if dry_run {
                    progress.file_deleted(key);
                } else {
                    s3_delete_keys.push(key.clone());
                }
            }
        }
    }

    if !dry_run && !uploads.is_empty() {
        let semaphore = Arc::new(Semaphore::new(concurrency));
        let s3 = s3.clone();
        let mut join_set = tokio::task::JoinSet::new();
        let completed = Arc::new(std::sync::Mutex::new(Vec::new()));

        for action in uploads {
            let permit = semaphore.clone().acquire_owned().await?;
            let s3 = s3.clone();
            let progress = progress.clone();
            let errors = errors.clone();
            let completed = completed.clone();

            join_set.spawn(async move {
                let _permit = permit;
                tracing::info!("Uploading: {}", action.key);
                let mut meta = HashMap::new();
                meta.insert(CACHE_MD5.to_string(), action.md5.clone());
                meta.insert(CACHE_MTIME.to_string(), action.mtime.to_string());
                if let Err(e) = s3.upload_object(&action.key, &action.local_path, Some(&meta)).await {
                    errors.lock().unwrap().push(SyncError::new(&action.key, e.to_string()));
                    tracing::error!("Failed to upload {}: {}", action.key, e);
                } else {
                        progress.file_uploaded(&action.key);
                        completed.lock().unwrap().push(action);
                    }
            });
        }

        while let Some(result) = join_set.join_next().await {
            if let Err(e) = result {
                tracing::error!("Task panicked: {}", e);
            }
        }

        for action in Arc::into_inner(completed).unwrap().into_inner().unwrap() {
            let etag = s3_map
                .get(&action.key)
                .map(|o| o.etag.clone())
                .unwrap_or_default();
            let last_modified = s3_map
                .get(&action.key)
                .map(|o| o.last_modified.to_rfc3339())
                .unwrap_or_default();
            cache.set(
                action.key,
                CacheEntry {
                    md5: action.md5,
                    mtime: action.mtime,
                    etag,
                    last_modified,
                },
            );
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
    progress.summary("Push complete");
    Ok(errs)
}

fn is_multipart(etag: &str) -> bool {
    etag.contains('-')
}
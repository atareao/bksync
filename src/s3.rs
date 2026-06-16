use std::collections::HashMap;
use std::fmt;
use std::path::Path;

use aws_runtime::auth::SigV4OperationSigningConfig;
use aws_sdk_s3::config::{BehaviorVersion, ConfigBag, Credentials, Region, RuntimeComponents};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use aws_sdk_s3::Client;
use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::interceptors::context::BeforeTransmitInterceptorContextMut;
use aws_smithy_runtime_api::client::interceptors::Intercept;
use chrono::{DateTime, Utc};
use tracing::instrument;

use crate::config::Profile;

pub const CACHE_MD5: &str = "source-md5";
pub const CACHE_MTIME: &str = "source-mtime";

#[derive(Clone)]
pub struct S3Object {
    pub key: String,
    pub etag: String,
    pub last_modified: DateTime<Utc>,
    pub size: i64,
}

#[derive(Clone)]
pub struct S3Client {
    client: Client,
    bucket: String,
}

#[derive(Debug)]
struct DisableSigningNormalization;

impl Intercept for DisableSigningNormalization {
    fn name(&self) -> &'static str {
        "DisableSigningNormalization"
    }

    fn modify_before_signing(
        &self,
        _context: &mut BeforeTransmitInterceptorContextMut<'_>,
        _runtime_components: &RuntimeComponents,
        cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        if let Some(mut config) = cfg.load::<SigV4OperationSigningConfig>().cloned() {
            config.signing_options.double_uri_encode = false;
            config.signing_options.normalize_uri_path = false;
            cfg.interceptor_state()
                .store_put::<SigV4OperationSigningConfig>(config);
        }
        Ok(())
    }
}

fn map_s3_error(op: &str, key: Option<&str>, err: impl fmt::Debug) -> anyhow::Error {
    let msg = format!("{} failed for '{}': {:?}", op, key.unwrap_or("?"), err);
    tracing::error!("{}", msg);
    anyhow::anyhow!("{}", msg)
}

impl S3Client {
    pub fn new(profile: &Profile) -> Self {
        let credentials = Credentials::new(
            &profile.access_key,
            &profile.secret_key,
            None,
            None,
            "bksync",
        );

        let region = profile.region.clone();
        let endpoint = profile.endpoint.clone();

        let mut config_builder = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(region))
            .endpoint_url(endpoint)
            .credentials_provider(credentials)
            .force_path_style(true);

        if profile.disable_path_normalization {
            config_builder = config_builder.interceptor(DisableSigningNormalization);
        }

        let config = config_builder.build();

        let client = Client::from_conf(config);

        Self {
            client,
            bucket: profile.bucket.clone(),
        }
    }

    #[instrument(skip(self))]
    pub async fn list_objects(&self, prefix: Option<&str>) -> anyhow::Result<Vec<S3Object>> {
        let mut objects = Vec::new();
        let mut continuation_token: Option<String> = None;
        let mut page = 0;

        tracing::info!("Listing objects in bucket '{}'...", self.bucket);

        loop {
            page += 1;
            let mut request = self.client.list_objects_v2().bucket(&self.bucket);
            if let Some(p) = prefix {
                request = request.prefix(p);
            }
            if let Some(token) = &continuation_token {
                request = request.continuation_token(token);
            }

            let response = request.send().await.map_err(|e| {
                map_s3_error("list_objects_v2", prefix, e)
            })?;

            let count = response.contents().len();
            for obj in response.contents() {
                let key = obj.key().unwrap_or_default().to_string();
                let etag = obj.e_tag().unwrap_or_default().trim_matches('"').to_string();
                let last_modified = obj.last_modified().copied().unwrap_or(aws_sdk_s3::primitives::DateTime::from_secs(0));

                let nanos = last_modified.as_nanos();
                let secs = (nanos / 1_000_000_000) as i64;
                let nsecs = (nanos % 1_000_000_000) as u32;
                let last_modified_dt: DateTime<Utc> = DateTime::from_timestamp(secs, nsecs).unwrap_or_default();
                let size = obj.size().unwrap_or(0);

                objects.push(S3Object {
                    key,
                    etag,
                    last_modified: last_modified_dt,
                    size,
                });
            }

            tracing::info!("  Page {}: {} objects ({} total so far)", page, count, objects.len());

            if response.is_truncated() == Some(true) {
                continuation_token = response
                    .next_continuation_token()
                    .map(|s| s.to_string());
            } else {
                break;
            }
        }

        tracing::info!("List complete: {} objects total", objects.len());
        Ok(objects)
    }

    #[instrument(skip(self))]
    pub async fn list_objects_map(&self, prefix: Option<&str>) -> anyhow::Result<HashMap<String, S3Object>> {
        let objects = self.list_objects(prefix).await?;
        let map: HashMap<String, S3Object> = objects.into_iter().map(|o| (o.key.clone(), o)).collect();
        Ok(map)
    }

    #[instrument(skip(self))]
    pub async fn download_object(&self, key: &str, local_path: &Path) -> anyhow::Result<()> {
        if let Some(parent) = local_path.parent() {
            match tokio::fs::create_dir_all(parent).await {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    tokio::fs::remove_file(parent).await?;
                    tokio::fs::create_dir_all(parent).await?;
                }
                Err(e) => return Err(e.into()),
            }
        }

        let response = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| map_s3_error("get_object", Some(key), e))?;

        let mut file = tokio::fs::File::create(local_path).await?;
        let mut reader = response.body.into_async_read();
        tokio::io::copy(&mut reader, &mut file).await?;

        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn upload_object(
        &self,
        key: &str,
        local_path: &Path,
        metadata: Option<&HashMap<String, String>>,
    ) -> anyhow::Result<()> {
        let body = ByteStream::from_path(local_path).await?;

        let mut req = self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(body);

        if let Some(meta) = metadata {
            for (k, v) in meta {
                req = req.metadata(k, v);
            }
        }

        req.send().await.map_err(|e| map_s3_error("put_object", Some(key), e))?;

        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn delete_object(&self, key: &str) -> anyhow::Result<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| map_s3_error("delete_object", Some(key), e))?;

        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn delete_objects(&self, keys: &[String]) -> anyhow::Result<()> {
        for chunk in keys.chunks(1000) {
            let mut delete = Delete::builder();
            for key in chunk {
                let obj = ObjectIdentifier::builder()
                    .key(key)
                    .build()?;
                delete = delete.objects(obj);
            }
            self.client
                .delete_objects()
                .bucket(&self.bucket)
                .delete(delete.build()?)
                .send()
                .await
                .map_err(|e| map_s3_error("delete_objects", None, e))?;
        }
        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn head_object_metadata(&self, key: &str) -> anyhow::Result<HashMap<String, String>> {
        let response = self.client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| map_s3_error("head_object", Some(key), e))?;

        Ok(response.metadata().cloned().unwrap_or_default())
    }

    #[instrument(skip(self))]
    pub async fn head_object(&self, key: &str) -> anyhow::Result<(String, DateTime<Utc>)> {
        let response = self.client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| map_s3_error("head_object", Some(key), e))?;

        let etag = response.e_tag().unwrap_or_default().trim_matches('"').to_string();
        let last_modified = response.last_modified()
            .copied()
            .unwrap_or(aws_sdk_s3::primitives::DateTime::from_secs(0));

        let nanos = last_modified.as_nanos();
        let secs = (nanos / 1_000_000_000) as i64;
        let nsecs = (nanos % 1_000_000_000) as u32;
        let last_modified_dt: DateTime<Utc> = DateTime::from_timestamp(secs, nsecs).unwrap_or_default();

        Ok((etag, last_modified_dt))
    }
}
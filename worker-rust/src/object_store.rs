use std::env;
use std::path::Path;

use chrono::Utc;
use futures::StreamExt;
use hmac::{Hmac, Mac};
use reqwest::header::{HeaderMap, HeaderValue, HOST};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

type HmacSha256 = Hmac<Sha256>;

fn sha256_hex(value: &[u8]) -> String {
    hex::encode(Sha256::digest(value))
}

fn sign(key: &[u8], value: &str) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts arbitrary keys");
    mac.update(value.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

pub async fn download_partition(
    object_key: &str,
    destination: &Path,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let endpoint = env::var("MINIO_ENDPOINT").unwrap_or_else(|_| "minio".to_owned());
    let port = env::var("MINIO_PORT").unwrap_or_else(|_| "9000".to_owned());
    let access_key = env::var("MINIO_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".to_owned());
    let secret_key = env::var("MINIO_SECRET_KEY").unwrap_or_else(|_| "minioadmin".to_owned());
    let host = format!("{endpoint}:{port}");
    let canonical_uri = format!("/partitions/{object_key}");
    let url = format!("http://{host}{canonical_uri}");
    let now = Utc::now();
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let date = now.format("%Y%m%d").to_string();
    let region = "us-east-1";
    let payload_hash = sha256_hex(b"");
    let canonical_headers =
        format!("host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n");
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";
    let canonical_request =
        format!("GET\n{canonical_uri}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}");
    let scope = format!("{date}/{region}/s3/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );
    let date_key = sign(format!("AWS4{secret_key}").as_bytes(), &date);
    let region_key = sign(&date_key, region);
    let service_key = sign(&region_key, "s3");
    let signing_key = sign(&service_key, "aws4_request");
    let signature = hex::encode(sign(&signing_key, &string_to_sign));
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
    );

    let mut headers = HeaderMap::new();
    headers.insert(HOST, HeaderValue::from_str(&host)?);
    headers.insert("x-amz-date", HeaderValue::from_str(&amz_date)?);
    headers.insert(
        "x-amz-content-sha256",
        HeaderValue::from_str(&payload_hash)?,
    );
    headers.insert("authorization", HeaderValue::from_str(&authorization)?);
    let response = reqwest::Client::new()
        .get(url)
        .headers(headers)
        .send()
        .await?;
    if !response.status().is_success() {
        return Err(format!("object download returned HTTP {}", response.status()).into());
    }

    let mut file = tokio::fs::File::create(destination).await?;
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        file.write_all(&chunk?).await?;
    }
    file.flush().await?;
    Ok(())
}

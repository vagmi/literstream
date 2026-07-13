//! Restores a database from a GCS replica using literstream, writing the image
//! to a file. Used by the litestream↔literstream cross-tool validation.
//!
//!     cargo run --example gcs_restore -- <bucket> <prefix> <out.db>

use std::sync::Arc;

use literstream::storage::ReplicaClient;
use literstream::sync::restore;
use object_store::gcp::GoogleCloudStorageBuilder;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 4 {
        eprintln!("usage: {} <bucket> <prefix> <out.db>", args[0]);
        std::process::exit(2);
    }
    let gcs = GoogleCloudStorageBuilder::from_env()
        .with_bucket_name(&args[1])
        .build()
        .expect("build gcs");
    let client = ReplicaClient::new(Arc::new(gcs), args[2].clone());

    let image = restore(&client).await.expect("restore");
    std::fs::write(&args[3], &image).expect("write out");
    println!("literstream restored {} bytes -> {}", image.len(), args[3]);
}

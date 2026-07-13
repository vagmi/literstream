//! Probes whether literstream can read a single page from a (possibly
//! litestream-produced) GCS replica via ranged reads — used to check LZ4
//! frame-format interop with pierrec/Go.
//!
//!     cargo run --example gcs_probe -- <bucket> <prefix>

use std::sync::Arc;

use literstream::storage::ReplicaClient;
use literstream::sync::ReplicaReader;
use object_store::gcp::GoogleCloudStorageBuilder;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let gcs = GoogleCloudStorageBuilder::from_env()
        .with_bucket_name(&args[1])
        .build()
        .expect("gcs");
    let client = ReplicaClient::new(Arc::new(gcs), args[2].clone());

    let mut reader = ReplicaReader::open(&client, None)
        .await
        .expect("open reader");
    println!("page_size = {}", reader.page_size());
    let page1 = reader
        .read_page(1)
        .await
        .expect("read page 1")
        .expect("page 1 exists");
    let magic = String::from_utf8_lossy(&page1[..16]);
    println!("page 1 first 16 bytes: {magic:?}");
    if magic.starts_with("SQLite format 3") {
        println!("OK: LZ4 frame interop works — decoded a valid SQLite page 1");
    } else {
        println!("MISMATCH: page 1 does not look like a SQLite header");
    }
}

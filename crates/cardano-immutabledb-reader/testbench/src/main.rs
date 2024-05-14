use std::{
    path::PathBuf,
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};

use cardano_immutabledb_reader::block_reader::{BlockReader, BlockReaderConfig};
use clap::Parser;
use pallas_traverse::MultiEraBlock;

#[derive(Parser)]
struct Cli {
    #[clap(short, long)]
    snapshot_immutabledb_path: PathBuf,
}

#[tokio::main]
async fn main() {
    let cli_args = Cli::parse();

    let byte_count = Arc::new(AtomicU64::new(0));
    let block_number = Arc::new(AtomicU64::new(0));

    let config = BlockReaderConfig {
        worker_read_buffer_bytes_size: 128 * 1024 * 1024,
        read_worker_count: 4,
        processing_worker_count: 4,
        worker_processing_buffer_bytes_size: 8 * 1024 * 1024,
    };

    let mut block_reader = BlockReader::new(cli_args.snapshot_immutabledb_path, &config, {
        let block_number = block_number.clone();
        let byte_count = byte_count.clone();

        move |block_data| {
            let decoded_data = MultiEraBlock::decode(block_data).expect("Decoded");
            byte_count.fetch_add(
                block_data.len() as u64,
                std::sync::atomic::Ordering::Acquire,
            );
            block_number.fetch_max(decoded_data.number(), std::sync::atomic::Ordering::Acquire);
        }
    })
    .await
    .expect("Block reader started");

    let mut ticker = tokio::time::interval(Duration::from_secs(1));

    loop {
        tokio::select! {
            _ = &mut block_reader => {
                break;
            }

            _ = ticker.tick() => {
                println!("BLOCK NUMBER {} | {} MB/s", block_number.load(std::sync::atomic::Ordering::Acquire), byte_count.swap(0, std::sync::atomic::Ordering::Acquire) / 1024 / 1024);
            }
        }
    }
}

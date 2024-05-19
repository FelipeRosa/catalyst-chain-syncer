use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering::Acquire},
        Arc,
    },
    time::Duration,
};

use cardano_immutabledb_reader::block_reader::{BlockReader, BlockReaderConfig};
use clap::Parser;
use pallas_traverse::MultiEraBlock;

fn parse_byte_size(s: &str) -> Result<u64, String> {
    parse_size::parse_size(s).map_err(|e| e.to_string())
}

#[derive(Parser)]
struct Cli {
    #[clap(short, long)]
    snapshot_immutabledb_path: PathBuf,
    #[clap(long, default_value_t = 2)]
    read_worker_count: usize,
    #[clap(long, default_value_t = 2)]
    processing_worker_count: usize,
    #[clap(long, value_parser = parse_byte_size, default_value = "128MiB")]
    read_worker_buffer_size: u64,
}

#[tokio::main]
async fn main() {
    let cli_args = Cli::parse();

    let byte_count = Arc::new(AtomicU64::new(0));
    let block_number = Arc::new(AtomicU64::new(0));

    let config = BlockReaderConfig {
        worker_read_buffer_bytes_size: cli_args.read_worker_buffer_size as usize,
        read_worker_count: cli_args.read_worker_count,
        processing_worker_count: cli_args.processing_worker_count,
    };

    let mut block_reader = BlockReader::new(cli_args.snapshot_immutabledb_path, &config, {
        let block_number = block_number.clone();
        let byte_count = byte_count.clone();

        move |block_data| {
            let decoded_data = MultiEraBlock::decode(&block_data).expect("Decoded");

            byte_count.fetch_add(block_data.len() as u64, Acquire);
            block_number.fetch_max(decoded_data.number(), Acquire);
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
                println!("BLOCK NUMBER {} | {} MB/s", block_number.load(Acquire), byte_count.swap(0, std::sync::atomic::Ordering::Acquire) / 1024 / 1024);
            }
        }
    }
}

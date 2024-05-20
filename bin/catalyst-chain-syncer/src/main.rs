use std::{
    error::Error,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering::Acquire},
        Arc,
    },
    time::{Duration, Instant},
};

use cardano_immutabledb_reader::block_reader::{BlockReader, BlockReaderConfig};
use catalyst_chaindata_writer::{
    writers::{
        cardano_block, cardano_spent_txo, cardano_transaction, cardano_txo, network::Network,
    },
    ChainDataWriter, WriteData,
};
use clap::Parser;
use pallas_traverse::MultiEraBlock;
use tokio::sync::mpsc;

fn parse_byte_size(s: &str) -> Result<u64, String> {
    parse_size::parse_size(s).map_err(|e| e.to_string())
}

#[derive(Parser)]
struct Cli {
    #[clap(long)]
    immutabledb_path: PathBuf,
    #[clap(long)]
    network: Network,

    #[clap(long)]
    database_url: String,

    #[clap(long, default_value_t = 1)]
    read_workers_count: usize,
    #[clap(long, value_parser = parse_byte_size, default_value = "128MiB")]
    read_worker_buffer_size: u64,
    #[clap(long, value_parser = parse_byte_size, default_value = "256MiB")]
    unprocessed_data_buffer_size: u64,
    #[clap(long, default_value_t = 1)]
    processing_workers_count: usize,
    #[clap(long, value_parser = parse_byte_size, default_value = "256MiB")]
    write_worker_buffer_size: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    catalyst_chaindata_writer::create_tables_if_not_present(&cli.database_url).await?;

    let mut t = Instant::now();

    let (writer, writer_handle) = ChainDataWriter::connect(
        cli.database_url.clone(),
        cli.write_worker_buffer_size as usize,
    )
    .await?;

    let reader_config = BlockReaderConfig {
        worker_read_buffer_byte_size: cli.read_worker_buffer_size as usize,
        read_worker_count: cli.read_workers_count,
        unprocessed_data_buffer_byte_size: cli.unprocessed_data_buffer_size as usize,
    };

    let mut reader = BlockReader::new(cli.immutabledb_path, &reader_config).await?;

    let latest_block_number = Arc::new(AtomicU64::new(0));
    let latest_slot_number = Arc::new(AtomicU64::new(0));
    let read_byte_count = Arc::new(AtomicU64::new(0));
    let processed_byte_count = Arc::new(AtomicU64::new(0));

    let mut processing_workers_txs = Vec::new();

    for _ in 0..cli.processing_workers_count {
        let (block_data_tx, mut block_data_rx) = mpsc::unbounded_channel::<(_, Vec<_>)>();
        processing_workers_txs.push(block_data_tx);

        let latest_block_number = latest_block_number.clone();
        let latest_slot_number = latest_slot_number.clone();
        let read_byte_count = read_byte_count.clone();
        let processed_byte_count = processed_byte_count.clone();
        let writer_handle = writer_handle.clone();

        tokio::task::spawn(async move {
            while let Some((_permit, block_bytes)) = block_data_rx.recv().await {
                read_byte_count.fetch_add(block_bytes.len() as u64, Acquire);

                let block = MultiEraBlock::decode(&block_bytes).expect("Decode");

                let Ok(block_data) = cardano_block::CardanoBlock::from_block(&block, cli.network)
                else {
                    eprintln!("Failed to parse block");
                    continue;
                };

                let Ok(transaction_data) =
                    cardano_transaction::CardanoTransaction::many_from_block(&block, cli.network)
                else {
                    eprintln!("Failed to parse transactions");
                    continue;
                };

                let txs = block.txs();

                let Ok(transaction_outputs_data) = cardano_txo::CardanoTxo::from_transactions(&txs)
                else {
                    eprintln!("Failed to parse TXOs");
                    continue;
                };

                let Ok(spent_transaction_outputs_data) =
                    cardano_spent_txo::CardanoSpentTxo::from_transactions(&txs)
                else {
                    eprintln!("Failed to parse spent TXOs");
                    continue;
                };

                let write_data = WriteData {
                    block: block_data,
                    transactions: transaction_data,
                    transaction_outputs: transaction_outputs_data,
                    spent_transaction_outputs: spent_transaction_outputs_data,
                };

                processed_byte_count.fetch_add(write_data.byte_size() as u64, Acquire);
                latest_block_number.fetch_max(block.number(), Acquire);
                latest_slot_number.fetch_max(block.slot(), Acquire);
                drop(block_bytes);

                writer_handle.write(write_data).await.expect("Write");
            }
        });
    }

    drop(writer_handle);

    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    let mut i = 0;

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("Stopping block readers and processing workers...");

                // TODO: Do this in a better way
                drop(reader);
                drop(processing_workers_txs);

                break;
            }

            res = reader.next() => {
                match res {
                    Some(v) => {
                        processing_workers_txs[i].send(v).expect("Worker");
                        i = (i + 1) % cli.processing_workers_count;
                    }
                    None => {
                        // TODO: Do this in a better way
                        drop(reader);
                        drop(processing_workers_txs);

                        break;
                    }
                }
            }

            _ = ticker.tick() => {
                let mem_usage = memory_stats::memory_stats().map(|s| s.physical_mem).unwrap_or(0);

                println!(
                    "BLOCK {} | SLOT {} | READ {} MB/s | PROCESSED {} MB/s | MEMORY USAGE {} MB",
                    latest_block_number.load(Acquire),
                    latest_slot_number.load(Acquire),
                    read_byte_count.swap(0, Acquire) / (1024 * 1024),
                    processed_byte_count.swap(0, Acquire) / (1024 * 1024),
                    mem_usage / (1024 * 1024),
                );
            }
        }
    }

    // Wait for the chain data writer to flush all data to postgres
    println!("Waiting writer...");
    writer.await?;
    println!("Finished syncing immutabledb data ({:?})", t.elapsed());

    println!("Creating indexes...");
    t = Instant::now();
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            println!("Exiting");
        }

        res = catalyst_chaindata_writer::create_indexes(&cli.database_url) => {
            res?;
            println!("Done (took {:?})", t.elapsed());
        }
    }

    Ok(())
}

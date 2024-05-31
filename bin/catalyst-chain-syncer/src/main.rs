mod server;

use std::{
    error::Error,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering::Acquire},
        Arc,
    },
    time::{Duration, Instant},
};

use cardano_immutabledb_reader::block_reader::{BlockReader, BlockReaderConfig};
use catalyst_chaindata_recover::Recoverer as _;
use catalyst_chaindata_types::{
    CardanoBlock, CardanoSpentTxo, CardanoTransaction, CardanoTxo, Network,
};
use catalyst_chaindata_writer::{ChainDataWriter, ChainDataWriterHandle, WriteData, Writer as _};
use clap::Parser;
use pallas_traverse::MultiEraBlock;
use tokio::sync::{mpsc, OwnedSemaphorePermit};
use tracing::{debug, info, warn};

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
    #[clap(long, value_parser = parse_byte_size, default_value = "64MiB")]
    unprocessed_data_buffer_size: u64,
    #[clap(long, default_value_t = 1)]
    processing_workers_count: usize,
    #[clap(long, value_parser = parse_byte_size, default_value = "32MiB")]
    write_worker_buffer_size: u64,

    #[clap(long, default_value = "127.0.0.1:8080")]
    api_listen_address: SocketAddr,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(tracing::level_filters::LevelFilter::INFO.into())
                .from_env()?,
        )
        .init();

    let cli = Cli::parse();

    let mut server_handle = server::start(cli.api_listen_address, &cli.database_url).await?;

    let mut pg_conn = postgres_store::Connection::open(&cli.database_url).await?;
    pg_conn.create_tables_if_not_present().await?;

    // Stats
    let latest_block_number = Arc::new(AtomicU64::new(0));
    let latest_slot_number = Arc::new(AtomicU64::new(0));
    let read_byte_count = Arc::new(AtomicU64::new(0));
    let processed_byte_count = Arc::new(AtomicU64::new(0));

    let mut processing_workers_txs = Vec::new();
    let mut writers = Vec::new();

    for _ in 0..cli.processing_workers_count {
        let pg_writer_conn = postgres_store::Connection::open(&cli.database_url).await?;

        let (writer, writer_handle) =
            ChainDataWriter::new(pg_writer_conn, cli.write_worker_buffer_size as usize).await?;
        writers.push(writer);

        let (block_data_tx, block_data_rx) = mpsc::unbounded_channel::<(_, Vec<_>)>();
        processing_workers_txs.push(block_data_tx);

        tokio::task::spawn(process_block_bytes(
            block_data_rx,
            cli.network,
            writer_handle,
            read_byte_count.clone(),
            processed_byte_count.clone(),
            latest_block_number.clone(),
            latest_slot_number.clone(),
        ));
    }

    let reader_config = BlockReaderConfig {
        worker_read_buffer_byte_size: cli.read_worker_buffer_size as usize,
        read_worker_count: cli.read_workers_count,
        unprocessed_data_buffer_byte_size: cli.unprocessed_data_buffer_size as usize,
    };

    let mut t = Instant::now();
    info!("Checking synced chain data...");

    let (missing_slot_ranges, latest_slot) = {
        let missing_slot_ranges = pg_conn.get_missing_slot_ranges().await?;
        let latest_slot = pg_conn.get_latest_slot().await?;

        (missing_slot_ranges, latest_slot)
    };

    if !missing_slot_ranges.is_empty() {
        info!("Found missing data ranges. Recovering.");

        for r in missing_slot_ranges {
            info!("Recovering range {r:?}");
            let mut reader =
                BlockReader::new(cli.immutabledb_path.clone(), &reader_config, r).await?;

            let mut i = 0;
            while let Some(v) = reader.next().await {
                processing_workers_txs[i].send(v).expect("Worker");
                i = (i + 1) % cli.processing_workers_count;
            }
        }
    }

    info!("Done (took {:?})", t.elapsed());

    t = Instant::now();
    let mut reader =
        BlockReader::new(cli.immutabledb_path, &reader_config, (latest_slot + 1)..).await?;

    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    let mut i = 0;
    let mut ctrl_c = false;

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Stopping block readers and processing workers...");

                // TODO: Do this in a better way
                drop(reader);
                drop(processing_workers_txs);

                ctrl_c = true;

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

                info!(
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

    // Wait for the chain data writers to flush all data to postgres
    info!("Waiting writers...");
    for writer in writers {
        writer.await?;
    }
    info!("Finished syncing immutabledb data ({:?})", t.elapsed());

    if ctrl_c {
        return Ok(());
    }

    info!("Creating indexes...");
    t = Instant::now();
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Exiting");
            ctrl_c = true;
        }

        res = pg_conn.create_indexes_if_not_present() => {
            res?;
            info!("Done (took {:?})", t.elapsed());
        }
    }

    if ctrl_c {
        return Ok(());
    }

    pg_conn.close().await?;

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Exiting");
        }

        _ = &mut server_handle => {}
    }

    server_handle.shutdown().await?;

    Ok(())
}

async fn process_block_bytes(
    mut block_data_rx: mpsc::UnboundedReceiver<(OwnedSemaphorePermit, Vec<u8>)>,
    network: Network,
    writer_handle: ChainDataWriterHandle,
    read_byte_count: Arc<AtomicU64>,
    processed_byte_count: Arc<AtomicU64>,
    latest_block_number: Arc<AtomicU64>,
    latest_slot_number: Arc<AtomicU64>,
) {
    while let Some((_permit, block_bytes)) = block_data_rx.recv().await {
        read_byte_count.fetch_add(block_bytes.len() as u64, Acquire);

        let block = MultiEraBlock::decode(&block_bytes).expect("Decode");

        let block_data = match CardanoBlock::from_block(&block, network) {
            Ok(data) => data,
            Err(e) => {
                warn!(error = ?e, "Failed to parse block");
                continue;
            }
        };

        let transaction_data = match CardanoTransaction::many_from_block(&block, network) {
            Ok(data) => data,
            Err(e) => {
                warn!(error = ?e, "Failed to parse transactions");
                continue;
            }
        };

        let txs = block.txs();

        let mut catalyst_registrations_data = Vec::new();
        for tx in &txs {
            let reg = match catalyst_chaindata_types::CatalystRegistration::from_transaction(
                tx, network,
            ) {
                Ok(Some(reg)) => reg,
                Ok(None) => continue,
                Err(e) => {
                    debug!(error = ?e, tx_hash = hex::encode(tx.hash()), "Failed to parse Catalyst registration");
                    continue;
                }
            };

            catalyst_registrations_data.push(reg);
        }

        let mut transaction_outputs_data = Vec::new();
        for tx in &txs {
            let data = CardanoTxo::from_transaction(tx);
            transaction_outputs_data.extend(data);
        }

        let mut spent_transaction_outputs_data = Vec::new();
        for tx in &txs {
            let data = CardanoSpentTxo::from_transaction(tx);
            spent_transaction_outputs_data.extend(data);
        }

        let write_data = WriteData {
            block: block_data,
            transactions: transaction_data,
            transaction_outputs: transaction_outputs_data,
            spent_transaction_outputs: spent_transaction_outputs_data,
            catalyst_registrations: catalyst_registrations_data,
        };

        // Update stats
        processed_byte_count.fetch_add(write_data.byte_size() as u64, Acquire);
        latest_block_number.fetch_max(block.number(), Acquire);
        latest_slot_number.fetch_max(block.slot(), Acquire);
        drop(block_bytes);

        writer_handle.write(write_data).await.expect("Write");
    }
}

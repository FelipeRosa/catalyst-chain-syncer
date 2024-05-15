use std::{
    error::Error,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering::Acquire},
        Arc,
    },
    time::Duration,
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

    #[clap(long, default_value_t = 2)]
    read_workers_count: usize,
    #[clap(long, default_value_t = 2)]
    processing_workers_count: usize,
    #[clap(long, value_parser = parse_byte_size, default_value = "128MiB")]
    read_worker_buffer_size: u64,
    #[clap(long, value_parser = parse_byte_size, default_value = "8MiB")]
    processing_worker_buffer_size: u64,
    #[clap(long, value_parser = parse_byte_size, default_value = "512MiB")]
    write_worker_buffer_size: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    catalyst_chaindata_writer::create_tables_if_not_present(&cli.database_url).await?;

    let (writer, writer_handle) = ChainDataWriter::connect(
        cli.database_url.clone(),
        cli.write_worker_buffer_size as usize,
    )
    .await?;

    let reader_config = BlockReaderConfig {
        worker_read_buffer_bytes_size: cli.read_worker_buffer_size as usize,
        read_worker_count: cli.read_workers_count,
        processing_worker_count: cli.processing_workers_count,
        worker_processing_buffer_bytes_size: cli.processing_worker_buffer_size as usize,
    };

    let latest_block_number = Arc::new(AtomicU64::new(0));
    let latest_slot_number = Arc::new(AtomicU64::new(0));
    let read_byte_count = Arc::new(AtomicU64::new(0));
    let write_byte_count = Arc::new(AtomicU64::new(0));

    let mut reader = BlockReader::new(cli.immutabledb_path, &reader_config, {
        let latest_block_number = latest_block_number.clone();
        let latest_slot_number = latest_slot_number.clone();
        let read_byte_count = read_byte_count.clone();
        let write_byte_count = write_byte_count.clone();

        move |block_bytes| {
            read_byte_count.fetch_add(block_bytes.len() as u64, Acquire);

            let mut w_byte_count = 0;

            let block = MultiEraBlock::decode(block_bytes).expect("Decode");

            let block_write_data = WriteData::Block(
                cardano_block::CardanoBlock::from_block(&block, cli.network).expect("Parse block"),
            );
            w_byte_count += block_write_data.data_size();
            writer_handle
                .blocking_write(block_write_data)
                .expect("Write");

            let tx_data =
                cardano_transaction::CardanoTransaction::many_from_block(&block, cli.network)
                    .expect("Parse transaction");
            for tx in tx_data {
                let tx_write_data = WriteData::Transaction(tx);
                w_byte_count += tx_write_data.data_size();

                writer_handle.blocking_write(tx_write_data).expect("Write");
            }

            let txs = block.txs();

            let txo_data = cardano_txo::CardanoTxo::from_transactions(&txs).expect("Parse txo");
            for txo in txo_data {
                let txo_write_data = WriteData::Txo(txo);
                w_byte_count += txo_write_data.data_size();

                writer_handle.blocking_write(txo_write_data).expect("Write");
            }

            let spent_txo_data = cardano_spent_txo::CardanoSpentTxo::from_transactions(&txs)
                .expect("Parse spent txo");
            for spent_txo in spent_txo_data {
                let spent_txo_write_data = WriteData::SpentTxo(spent_txo);
                w_byte_count += spent_txo_write_data.data_size();

                writer_handle
                    .blocking_write(spent_txo_write_data)
                    .expect("Write");
            }

            latest_block_number.fetch_max(block.number(), Acquire);
            latest_slot_number.fetch_max(block.slot(), Acquire);
            write_byte_count.fetch_add(w_byte_count as u64, Acquire);
        }
    })
    .await?;

    let mut ticker = tokio::time::interval(Duration::from_secs(1));

    loop {
        tokio::select! {
            res = &mut reader => {
                res?;
                break;
            }

            _ = ticker.tick() => {
                println!(
                    "BLOCK {} | SLOT {} | READ {} MB/s | WRITE {} MB/s",
                    latest_block_number.load(Acquire),
                    latest_slot_number.load(Acquire),
                    read_byte_count.swap(0, Acquire) / (1024 * 1024),
                    write_byte_count.swap(0, Acquire) / (1024 * 1024),
                );
            }
        }
    }

    writer.finish().await?;
    catalyst_chaindata_writer::create_indexes(&cli.database_url).await?;

    Ok(())
}

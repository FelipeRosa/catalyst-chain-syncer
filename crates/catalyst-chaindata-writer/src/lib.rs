pub mod connection;
pub mod writers;

use std::{future::Future, sync::Arc};

use anyhow::Result;
use connection::Connection;
use tokio::{
    sync::{mpsc, OwnedSemaphorePermit, Semaphore},
    task::{JoinError, JoinHandle},
};
use writers::{
    cardano_block::CardanoBlock, cardano_spent_txo::CardanoSpentTxo,
    cardano_transaction::CardanoTransaction, cardano_txo::CardanoTxo,
};

pub async fn create_tables_if_not_present(conn_string: &str) -> Result<()> {
    let conn = Connection::open(conn_string).await?;

    conn.client()
        .batch_execute(include_str!("../../../sql/create_tables.sql"))
        .await?;
    conn.close().await?;

    Ok(())
}

pub async fn create_indexes(conn_string: &str) -> Result<()> {
    let conn = Connection::open(conn_string).await?;

    conn.client()
        .batch_execute(include_str!("../../../sql/create_indexes.sql"))
        .await?;
    conn.close().await?;

    Ok(())
}

pub enum WriteData {
    Block(CardanoBlock),
    Transaction(CardanoTransaction),
    Txo(CardanoTxo),
    SpentTxo(CardanoSpentTxo),
}

impl WriteData {
    pub fn data_size(&self) -> usize {
        match self {
            WriteData::Block(b) => std::mem::size_of_val(b),
            WriteData::Transaction(tx) => std::mem::size_of_val(tx),
            WriteData::Txo(txo) => std::mem::size_of_val(txo) + txo.assets_size_estimate,
            WriteData::SpentTxo(spent_txo) => std::mem::size_of_val(spent_txo),
        }
    }
}

#[derive(Clone)]
pub struct ChainDataWriterHandle {
    write_semaphore: Arc<Semaphore>,
    write_data_tx: mpsc::UnboundedSender<(OwnedSemaphorePermit, WriteData)>,
}

impl ChainDataWriterHandle {
    pub fn blocking_write(&self, d: WriteData) -> Result<()> {
        let rt = tokio::runtime::Handle::current();

        let permit = rt.block_on(
            self.clone()
                .write_semaphore
                .acquire_many_owned(d.data_size() as u32),
        )?;

        self.write_data_tx.send((permit, d))?;

        Ok(())
    }
}

pub struct ChainDataWriter {
    write_worker_task_handle: JoinHandle<()>,
}

impl ChainDataWriter {
    pub async fn connect(
        conn_string: String,
        write_buffer_byte_size: usize,
    ) -> Result<(Self, ChainDataWriterHandle)> {
        let (write_data_tx, write_data_rx) = mpsc::unbounded_channel();

        let write_worker_task_handle = tokio::spawn(write_task::start(
            conn_string,
            write_buffer_byte_size,
            write_data_rx,
        ));

        let this = Self {
            write_worker_task_handle,
        };

        let handle = ChainDataWriterHandle {
            // Explain
            write_semaphore: Arc::new(Semaphore::new(write_buffer_byte_size * 2)),
            write_data_tx,
        };

        Ok((this, handle))
    }
}

impl Future for ChainDataWriter {
    type Output = Result<(), JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut write_worker_task_handle = std::pin::pin!(&mut self.write_worker_task_handle);
        write_worker_task_handle.as_mut().poll(cx)
    }
}

mod write_task {
    use std::time::Duration;

    use tokio::sync::{mpsc, OwnedSemaphorePermit};

    use crate::{
        connection::Connection,
        writers::{
            cardano_block, cardano_spent_txo, cardano_transaction, cardano_txo, Writer as _,
        },
        WriteData,
    };

    pub async fn start(
        conn_string: String,
        write_buffer_byte_size: usize,
        mut write_data_rx: mpsc::UnboundedReceiver<(OwnedSemaphorePermit, WriteData)>,
    ) {
        let block_writer =
            cardano_block::Writer::new(Connection::open(&conn_string).await.expect("Connection"));
        let tx_writer = cardano_transaction::Writer::new(
            Connection::open(&conn_string).await.expect("Connection"),
        );
        let txo_writer =
            cardano_txo::Writer::new(Connection::open(&conn_string).await.expect("Connection"));
        let spent_txo_writer = cardano_spent_txo::Writer::new(
            Connection::open(&conn_string).await.expect("Connection"),
        );

        let mut block_buffer: Vec<cardano_block::CardanoBlock> = Vec::new();
        let mut tx_buffer = Vec::new();
        let mut txo_buffer = Vec::new();
        let mut spent_txo_buffer = Vec::new();

        let mut merged_permits: Option<OwnedSemaphorePermit> = None;
        let mut total_byte_count = 0;

        let mut flush = false;
        let mut close = false;

        let mut ticker = tokio::time::interval(Duration::from_secs(30));

        loop {
            tokio::select! {
                // If we did not receive data for a while, flush the buffers.
                _ = ticker.tick() => {
                    flush = true;
                }

                res = write_data_rx.recv() => {
                    // Reset the ticker since we received data.
                    ticker.reset();

                    match res {
                        None => {
                            flush = true;
                            close = true;
                        }
                        Some((permit, data)) => {
                            total_byte_count += data.data_size();

                            match merged_permits.as_mut() {
                                Some(p) => p.merge(permit),
                                None => merged_permits = Some(permit),
                            }

                            match data {
                                WriteData::Block(b) => {
                                    block_buffer.push(b);
                                }
                                WriteData::Transaction(tx) => {
                                    tx_buffer.push(tx);
                                }
                                WriteData::Txo(txo) => {
                                    txo_buffer.push(txo);
                                }
                                WriteData::SpentTxo(spent_txo) => {
                                    spent_txo_buffer.push(spent_txo);
                                }
                            }
                        }
                    }
                }
            }

            if (flush && total_byte_count > 0) || total_byte_count >= write_buffer_byte_size {
                println!(
                    "WRITING {:?} | SIZE {:.2} MB",
                    block_buffer.iter().map(|b| b.block_no).max(),
                    (total_byte_count as f64) / (1024.0 * 1024.0),
                );

                tokio::try_join!(
                    block_writer.batch_copy(&block_buffer),
                    tx_writer.batch_copy(&tx_buffer),
                    txo_writer.batch_copy(&txo_buffer),
                    spent_txo_writer.batch_copy(&spent_txo_buffer)
                )
                .expect("Batch copies");

                // block_buffer = Vec::new();
                // tx_buffer = Vec::new();
                // txo_buffer = Vec::new();
                // spent_txo_buffer = Vec::new();

                total_byte_count = 0;
                merged_permits = None;

                ticker.reset();

                flush = false;
            }

            if close {
                break;
            }
        }
    }
}

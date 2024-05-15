pub mod connection;
pub mod writers;

use std::{error::Error, sync::Arc};

use connection::Connection;
use tokio::{
    sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore},
    task::{JoinError, JoinHandle},
};
use writers::{
    cardano_block::CardanoBlock, cardano_spent_txo::CardanoSpentTxo,
    cardano_transaction::CardanoTransaction, cardano_txo::CardanoTxo,
};

pub async fn create_tables_if_not_present(conn_string: &str) -> Result<(), Box<dyn Error>> {
    let conn = Connection::open(conn_string).await?;

    conn.client()
        .batch_execute(include_str!("../sql/create_tables.sql"))
        .await?;
    conn.close().await?;

    Ok(())
}

pub async fn create_indexes(conn_string: &str) -> Result<(), Box<dyn Error>> {
    let conn = Connection::open(conn_string).await?;

    conn.client()
        .batch_execute(include_str!("../sql/create_indexes.sql"))
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
            WriteData::Txo(txo) => std::mem::size_of_val(txo),
            WriteData::SpentTxo(spent_txo) => std::mem::size_of_val(spent_txo),
        }
    }
}

#[derive(Clone)]
pub struct ChainDataWriterHandle {
    write_data_semaphore: Arc<Semaphore>,
    write_data_tx: mpsc::UnboundedSender<(OwnedSemaphorePermit, WriteData)>,
}

impl ChainDataWriterHandle {
    pub fn blocking_write(&self, d: WriteData) -> anyhow::Result<()> {
        let rt = tokio::runtime::Handle::current();

        let permit = rt.block_on(
            self.write_data_semaphore
                .clone()
                .acquire_many_owned(d.data_size() as u32),
        )?;

        self.write_data_tx.send((permit, d))?;

        Ok(())
    }
}

pub struct ChainDataWriter {
    write_worker_task_handle: JoinHandle<()>,
    cancel: oneshot::Receiver<()>,
}

impl ChainDataWriter {
    pub async fn connect(
        conn_string: String,
        write_buffer_byte_size: usize,
    ) -> Result<(Self, ChainDataWriterHandle), Box<dyn Error>> {
        let (write_data_tx, write_data_rx) = mpsc::unbounded_channel();
        let (cancel_tx, cancel_rx) = oneshot::channel();

        let write_worker_task_handle = tokio::spawn(write_task::start(
            conn_string,
            write_buffer_byte_size,
            write_data_rx,
            cancel_tx,
        ));

        let write_data_semaphore = Arc::new(Semaphore::new(write_buffer_byte_size));

        let this = Self {
            write_worker_task_handle,
            cancel: cancel_rx,
        };

        let handle = ChainDataWriterHandle {
            write_data_semaphore,
            write_data_tx,
        };

        Ok((this, handle))
    }

    pub async fn finish(self) -> Result<(), JoinError> {
        drop(self.cancel);
        self.write_worker_task_handle.await
    }
}

mod write_task {
    use std::time::Duration;

    use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit};

    use crate::{
        connection::Connection,
        writers::{cardano_block, cardano_spent_txo, cardano_transaction, cardano_txo, Writer},
        WriteData,
    };

    pub async fn start(
        conn_string: String,
        write_buffer_byte_size: usize,
        mut write_data_rx: mpsc::UnboundedReceiver<(OwnedSemaphorePermit, WriteData)>,
        mut cancel: oneshot::Sender<()>,
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

        let mut block_buffer = Vec::new();
        let mut tx_buffer = Vec::new();
        let mut txo_buffer = Vec::new();
        let mut spent_txo_buffer = Vec::new();
        let mut total_byte_count = 0;

        let mut write_permit: Option<OwnedSemaphorePermit> = None;
        let mut flush = false;
        let mut close = false;

        let mut ticker = tokio::time::interval(Duration::from_secs(30));

        while !close {
            tokio::select! {
                _ = cancel.closed() => {
                    flush = true;
                    close = true;

                    while let Ok((permit, data)) = write_data_rx.try_recv() {
                        match write_permit.as_mut() {
                            Some(current) => {
                                current.merge(permit);
                            }
                            None => write_permit = Some(permit),
                        }

                        match data {
                            WriteData::Block(b) => {
                                total_byte_count += std::mem::size_of_val(&b);
                                block_buffer.push(b);
                            }
                            WriteData::Transaction(tx) => {
                                total_byte_count += std::mem::size_of_val(&tx);
                                tx_buffer.push(tx);
                            }
                            WriteData::Txo(txo) => {
                                total_byte_count += std::mem::size_of_val(&txo);
                                txo_buffer.push(txo);
                            }
                            WriteData::SpentTxo(spent_txo) => {
                                total_byte_count += std::mem::size_of_val(&spent_txo);
                                spent_txo_buffer.push(spent_txo);
                            }
                        }
                    }
                }

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
                            match write_permit.as_mut() {
                                Some(current) => {
                                    current.merge(permit);
                                }
                                None => write_permit = Some(permit),
                            }

                            match data {
                                WriteData::Block(b) => {
                                    total_byte_count += std::mem::size_of_val(&b);
                                    block_buffer.push(b);
                                }
                                WriteData::Transaction(tx) => {
                                    total_byte_count += std::mem::size_of_val(&tx);
                                    tx_buffer.push(tx);
                                }
                                WriteData::Txo(txo) => {
                                    total_byte_count += std::mem::size_of_val(&txo);
                                    txo_buffer.push(txo);
                                }
                                WriteData::SpentTxo(spent_txo) => {
                                    total_byte_count += std::mem::size_of_val(&spent_txo);
                                    spent_txo_buffer.push(spent_txo);
                                }
                            }
                        }
                    }
                }
            }

            if (flush && total_byte_count > 0)
                || total_byte_count >= write_buffer_byte_size - std::mem::size_of::<WriteData>()
            {
                println!(
                    "WRITING {:?}",
                    block_buffer.iter().map(|b| b.block_no).max()
                );
                tokio::try_join!(
                    block_writer.batch_copy(&block_buffer),
                    tx_writer.batch_copy(&tx_buffer),
                    txo_writer.batch_copy(&txo_buffer),
                    spent_txo_writer.batch_copy(&spent_txo_buffer)
                )
                .expect("Batch copies");

                total_byte_count = 0;

                block_buffer.clear();
                tx_buffer.clear();
                txo_buffer.clear();
                spent_txo_buffer.clear();

                write_permit = None;
            }

            flush = false;
        }
    }
}

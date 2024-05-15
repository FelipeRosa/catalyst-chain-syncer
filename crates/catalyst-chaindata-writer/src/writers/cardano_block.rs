use std::error::Error;

use chrono::{DateTime, Utc};
use pallas_traverse::MultiEraBlock;
use tokio::task::JoinError;
use tokio_postgres::{binary_copy::BinaryCopyInWriter, types::Type};

use crate::connection::Connection;

use super::network::Network;

#[derive(Debug, Clone, Copy)]
pub struct CardanoBlock {
    pub block_no: u64,
    pub slot_no: u64,
    pub epoch_no: u64,
    pub network: Network,
    pub block_time: DateTime<Utc>,
    pub block_hash: [u8; 32],
    pub previous_hash: Option<[u8; 32]>,
}

impl CardanoBlock {
    pub fn from_block(block: &MultiEraBlock, network: Network) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            block_no: block.number(),
            slot_no: block.slot(),
            epoch_no: block.epoch(network.genesis_values()).0,
            network,
            block_time: DateTime::from_timestamp(
                block.wallclock(network.genesis_values()) as i64,
                0,
            )
            .ok_or_else(|| "Failed to parse DateTime from timestamp".to_string())?,
            block_hash: *block.hash(),
            previous_hash: block.header().previous_hash().as_ref().map(|h| **h),
        })
    }
}

pub struct Writer {
    conn: Connection,
}

impl Writer {
    pub fn new(conn: Connection) -> Self {
        Self { conn }
    }

    pub async fn close(self) -> Result<(), JoinError> {
        self.conn.close().await
    }
}

impl super::Writer for Writer {
    type In = CardanoBlock;

    async fn batch_copy(&self, data: &[Self::In]) -> anyhow::Result<()> {
        let sink = self.conn.client().copy_in("COPY cardano_blocks (block_no, slot_no, epoch_no, network_id, block_time, block_hash, previous_hash) FROM STDIN BINARY").await?;

        let writer = BinaryCopyInWriter::new(
            sink,
            &[
                Type::INT8,
                Type::INT8,
                Type::INT8,
                Type::INT2,
                Type::TIMESTAMPTZ,
                Type::BYTEA,
                Type::BYTEA,
            ],
        );
        tokio::pin!(writer);

        for cb in data {
            writer
                .as_mut()
                .write(&[
                    &(cb.block_no as i64),
                    &(cb.slot_no as i64),
                    &(cb.epoch_no as i64),
                    &(cb.network.id() as i16),
                    &cb.block_time,
                    &cb.block_hash.as_slice(),
                    &cb.previous_hash.as_ref().map(|h| h.as_slice()),
                ])
                .await?;
        }

        writer.finish().await?;

        Ok(())
    }
}

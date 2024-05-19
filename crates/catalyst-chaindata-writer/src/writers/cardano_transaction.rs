use pallas_traverse::MultiEraBlock;
use tokio::task::JoinError;
use tokio_postgres::{binary_copy::BinaryCopyInWriter, types::Type};

use crate::connection::Connection;

use super::network::Network;

pub struct CardanoTransaction {
    hash: [u8; 32],
    block_no: u64,
    network: Network,
}

impl CardanoTransaction {
    pub fn many_from_block(block: &MultiEraBlock, network: Network) -> anyhow::Result<Vec<Self>> {
        let data = block
            .txs()
            .into_iter()
            .map(|tx| Self {
                hash: *tx.hash(),
                block_no: block.number(),
                network,
            })
            .collect();

        Ok(data)
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
    type In = CardanoTransaction;

    async fn batch_copy(&self, data: &[Self::In]) -> anyhow::Result<()> {
        let sink = self
            .conn
            .client()
            .copy_in("COPY cardano_transactions (block_no, network_id, hash) FROM STDIN BINARY")
            .await?;
        let writer = BinaryCopyInWriter::new(sink, &[Type::INT8, Type::INT2, Type::BYTEA]);
        tokio::pin!(writer);

        for tx_data in data {
            writer
                .as_mut()
                .write(&[
                    &(tx_data.block_no as i64),
                    &(tx_data.network.id() as i16),
                    &tx_data.hash.as_slice(),
                ])
                .await
                .expect("WRITE");
        }

        writer.finish().await.expect("FINISH");

        Ok(())
    }
}

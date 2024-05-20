use db_util::connection::{
    tokio_postgres::{binary_copy::BinaryCopyInWriter, types::Type},
    Connection,
};
use pallas_traverse::MultiEraTx;
use tokio::task::JoinError;

pub struct CardanoSpentTxo {
    from_transaction_hash: [u8; 32],
    index: u32,
    to_transaction_hash: [u8; 32],
}

impl CardanoSpentTxo {
    pub fn from_transactions(txs: &[MultiEraTx]) -> anyhow::Result<Vec<Self>> {
        let data = txs
            .iter()
            .flat_map(|tx| {
                tx.inputs().into_iter().map(|tx_input| Self {
                    from_transaction_hash: **tx_input.output_ref().hash(),
                    index: tx_input.output_ref().index() as u32,
                    to_transaction_hash: *tx.hash(),
                })
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
    type In = CardanoSpentTxo;

    async fn batch_copy(&self, data: &[Self::In]) -> anyhow::Result<()> {
        let sink = self.conn.client()
                    .copy_in("COPY cardano_spent_txo (from_transaction_hash, index, to_transaction_hash) FROM STDIN BINARY")
                    .await
                    .expect("COPY");
        let writer = BinaryCopyInWriter::new(sink, &[Type::BYTEA, Type::INT4, Type::BYTEA]);
        tokio::pin!(writer);

        for spent_txo_data in data {
            writer
                .as_mut()
                .write(&[
                    &spent_txo_data.from_transaction_hash.as_slice(),
                    &(spent_txo_data.index as i32),
                    &spent_txo_data.to_transaction_hash.as_slice(),
                ])
                .await
                .expect("WRITE");
        }

        writer.finish().await.expect("FINISH");

        Ok(())
    }
}

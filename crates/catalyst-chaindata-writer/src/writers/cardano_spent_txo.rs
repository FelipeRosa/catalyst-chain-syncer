use catalyst_chaindata_types::CardanoSpentTxo;
use db_util::connection::{
    tokio_postgres::{binary_copy::BinaryCopyInWriter, types::Type},
    Connection,
};
use tokio::task::JoinError;

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

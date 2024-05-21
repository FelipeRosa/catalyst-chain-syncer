use anyhow::Result;
use catalyst_chaindata_types::CardanoTxo;
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
    type In = CardanoTxo;

    async fn batch_copy(&self, data: &[Self::In]) -> Result<()> {
        let sink = self
            .conn
            .client()
            .copy_in("COPY cardano_txo (transaction_hash, index, value, assets, stake_credential) FROM STDIN BINARY")
            .await
            .expect("COPY");
        let writer = BinaryCopyInWriter::new(
            sink,
            &[
                Type::BYTEA,
                Type::INT4,
                Type::INT8,
                Type::JSONB,
                Type::BYTEA,
            ],
        );
        tokio::pin!(writer);

        for txo_data in data {
            writer
                .as_mut()
                .write(&[
                    &txo_data.transaction_hash.as_slice(),
                    &(txo_data.index as i32),
                    &(txo_data.value as i64),
                    &txo_data.assets,
                    &txo_data.stake_credential.as_ref().map(|a| a.as_slice()),
                ])
                .await
                .expect("WRITE");
        }

        writer.finish().await.expect("FINISH");

        Ok(())
    }
}

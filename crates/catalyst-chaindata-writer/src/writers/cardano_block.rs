use catalyst_chaindata_types::CardanoBlock;
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

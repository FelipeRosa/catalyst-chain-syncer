use std::future::Future;

use catalyst_chaindata_recover::Recoverer;
use catalyst_chaindata_types::{
    CardanoBlock, CardanoSpentTxo, CardanoTransaction, CardanoTxo, CatalystRegistration,
    CatalystRegistrationVotingKey,
};
use catalyst_chaindata_writer::{WriteData, Writer};
use tokio_postgres::{binary_copy::BinaryCopyInWriter, types::Type};

pub struct Connection {
    client: tokio_postgres::Client,
    conn_task_handle: tokio::task::JoinHandle<()>,
}

impl Connection {
    pub async fn open(conn_string: &str) -> anyhow::Result<Self> {
        let (client, conn) = tokio_postgres::connect(conn_string, tokio_postgres::NoTls).await?;

        let conn_task_handle = tokio::spawn(async move {
            conn.await.expect("Success");
        });

        Ok(Self {
            client,
            conn_task_handle,
        })
    }

    pub fn client(&self) -> &tokio_postgres::Client {
        &self.client
    }

    pub fn client_mut(&mut self) -> &mut tokio_postgres::Client {
        &mut self.client
    }

    pub async fn create_tables_if_not_present(&self) -> anyhow::Result<()> {
        self.client()
            .batch_execute(include_str!("../sql/create_tables.sql"))
            .await?;

        Ok(())
    }

    pub async fn create_indexes_if_not_present(&self) -> anyhow::Result<()> {
        self.client()
            .batch_execute(include_str!("../sql/create_indexes.sql"))
            .await?;

        Ok(())
    }
}

impl Recoverer for Connection {
    async fn get_missing_slot_ranges(
        &mut self,
    ) -> anyhow::Result<Vec<catalyst_chaindata_recover::SlotRange>> {
        let rows = self
            .client()
            .query(include_str!("../sql/find_missing_data.sql"), &[])
            .await?;

        let mut missing_data_ranges = Vec::new();
        for row in rows {
            let start_slot_no = row.get::<_, i64>(0) as u64;
            let end_slot_no = row.get::<_, i64>(1) as u64;

            missing_data_ranges.push((start_slot_no + 1)..end_slot_no);
        }

        Ok(missing_data_ranges)
    }

    async fn get_latest_slot(&mut self) -> anyhow::Result<u64> {
        let row = self
            .client()
            .query_opt(include_str!("../sql/latest_slot.sql"), &[])
            .await?;

        let latest_slot = match row {
            Some(row) => row.get::<_, i64>(0) as u64,
            None => 0,
        };

        Ok(latest_slot)
    }
}

impl Writer for Connection {
    async fn batch_write(&mut self, data: Vec<WriteData>) -> anyhow::Result<()> {
        let mut blocks = Vec::new();
        let mut transactions = Vec::new();
        let mut txos = Vec::new();
        let mut spent_txos = Vec::new();
        let mut registrations = Vec::new();

        for d in data {
            blocks.push(d.block);
            transactions.extend(d.transactions);
            txos.extend(d.transaction_outputs);
            spent_txos.extend(d.spent_transaction_outputs);
            registrations.extend(d.catalyst_registrations);
        }

        let tx = self.client_mut().transaction().await?;

        tokio::try_join!(
            copy_blocks(&tx, &blocks),
            copy_transactions(&tx, &transactions),
            copy_txos(&tx, &txos),
            copy_spent_txos(&tx, &spent_txos),
            copy_registrations(&tx, &registrations)
        )?;

        tx.commit().await?;

        Ok(())
    }

    fn close(self) -> impl Future<Output = anyhow::Result<()>> {
        let join_handle = self.conn_task_handle;

        async move { join_handle.await.map_err(|err| anyhow::anyhow!(err)) }
    }
}

async fn copy_blocks(
    tx: &tokio_postgres::Transaction<'_>,
    data: &[CardanoBlock],
) -> anyhow::Result<()> {
    let sink = tx.copy_in("COPY cardano_blocks (block_no, slot_no, epoch_no, network_id, block_time, block_hash, previous_hash) FROM STDIN BINARY").await?;

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

async fn copy_transactions(
    tx: &tokio_postgres::Transaction<'_>,
    data: &[CardanoTransaction],
) -> anyhow::Result<()> {
    let sink = tx
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

async fn copy_txos(
    tx: &tokio_postgres::Transaction<'_>,
    data: &[CardanoTxo],
) -> anyhow::Result<()> {
    let sink = tx
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

async fn copy_spent_txos(
    tx: &tokio_postgres::Transaction<'_>,
    data: &[CardanoSpentTxo],
) -> anyhow::Result<()> {
    let sink = tx.
                    copy_in("COPY cardano_spent_txo (from_transaction_hash, index, to_transaction_hash) FROM STDIN BINARY")
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

async fn copy_registrations(
    tx: &tokio_postgres::Transaction<'_>,
    data: &[CatalystRegistration],
) -> anyhow::Result<()> {
    let sink = tx.copy_in("COPY catalyst_registrations (transaction_hash, voting_key, voting_key_weight, stake_public_key, stake_credential, payment_address, nonce, voting_purpose) FROM STDIN BINARY")
        .await
        .expect("COPY");
    let writer = BinaryCopyInWriter::new(
        sink,
        &[
            Type::BYTEA,
            Type::BYTEA_ARRAY,
            Type::INT4_ARRAY,
            Type::BYTEA,
            Type::BYTEA,
            Type::BYTEA,
            Type::INT8,
            Type::INT4,
        ],
    );
    tokio::pin!(writer);

    for registration_data in data {
        let (voting_keys, voting_key_weights) = match &registration_data.voting_key {
            CatalystRegistrationVotingKey::Legacy(vk) => (vec![vk.as_slice()], vec![1]),
            CatalystRegistrationVotingKey::Delegations(ds) => {
                let voting_keys = ds.iter().map(|d| d.0.as_slice()).collect();
                let voting_key_weights = ds.iter().map(|d| d.1 as i32).collect();

                (voting_keys, voting_key_weights)
            }
        };

        writer
            .as_mut()
            .write(&[
                &registration_data.transaction_hash.as_slice(),
                &voting_keys,
                &voting_key_weights,
                &registration_data.stake_public_key.as_slice(),
                &registration_data.stake_credential.as_slice(),
                &registration_data.payment_address.as_slice(),
                &(registration_data.nonce as i64),
                &registration_data
                    .voting_purpose
                    .map(|vp| vp as i32)
                    .unwrap_or_default(),
            ])
            .await
            .expect("WRITE");
    }

    writer.finish().await.expect("FINISH");

    Ok(())
}

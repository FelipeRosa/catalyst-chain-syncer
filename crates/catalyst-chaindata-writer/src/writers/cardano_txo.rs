use anyhow::Result;
use db_util::connection::{
    tokio_postgres::{binary_copy::BinaryCopyInWriter, types::Type},
    Connection,
};
use pallas_traverse::{MultiEraAsset, MultiEraPolicyAssets, MultiEraTx};
use serde::Serialize;
use tokio::task::JoinError;

use super::serde_size::serde_size;

pub struct CardanoTxo {
    pub transaction_hash: [u8; 32],
    pub index: u32,
    pub value: u64,
    pub assets: serde_json::Value,
    pub assets_size_estimate: usize,
    pub stake_credential: Option<[u8; 28]>,
}

impl CardanoTxo {
    pub fn from_transactions(txs: &[MultiEraTx]) -> Result<Vec<Self>> {
        let data = txs
            .iter()
            .flat_map(|tx| {
                tx.outputs().into_iter().zip(0..).map(|(tx_output, index)| {
                    let address = tx_output.address()?;

                    let stake_credential = match address {
                        pallas_addresses::Address::Byron(_) => None,
                        pallas_addresses::Address::Shelley(address) => address.try_into().ok(),
                        pallas_addresses::Address::Stake(stake_address) => Some(stake_address),
                    };

                    let parsed_assets = parse_policy_assets(&tx_output.non_ada_assets());
                    let assets_size_estimate = serde_size(&parsed_assets)?;

                    Ok(Self {
                        transaction_hash: *tx.hash(),
                        index,
                        value: tx_output.lovelace_amount(),
                        assets: serde_json::to_value(parsed_assets)?,
                        assets_size_estimate,
                        stake_credential: stake_credential.map(|a| **a.payload().as_hash()),
                    })
                })
            })
            .collect::<Result<Vec<_>>>()?;

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

#[derive(Debug, Serialize)]
/// Assets
struct Asset {
    /// Policy id
    pub policy_id: String,
    /// Asset name
    pub name: String,
    /// Amount in lovelace
    pub amount: u64,
}

#[derive(Debug, Serialize)]
struct PolicyAsset {
    /// Policy identifier
    pub policy_hash: String,
    /// All policy assets
    pub assets: Vec<Asset>,
}

fn parse_policy_assets(assets: &[MultiEraPolicyAssets<'_>]) -> Vec<PolicyAsset> {
    assets
        .iter()
        .map(|asset| PolicyAsset {
            policy_hash: asset.policy().to_string(),
            assets: parse_child_assets(&asset.assets()),
        })
        .collect()
}

fn parse_child_assets(assets: &[MultiEraAsset]) -> Vec<Asset> {
    assets
        .iter()
        .filter_map(|asset| match asset {
            MultiEraAsset::AlonzoCompatibleOutput(id, name, amount) => Some(Asset {
                policy_id: id.to_string(),
                name: name.to_string(),
                amount: *amount,
            }),
            MultiEraAsset::AlonzoCompatibleMint(id, name, amount) => {
                let amount = u64::try_from(*amount).ok()?;
                Some(Asset {
                    policy_id: id.to_string(),
                    name: name.to_string(),
                    amount,
                })
            }
            _ => None,
        })
        .collect()
}

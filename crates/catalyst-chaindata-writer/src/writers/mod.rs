use std::future::Future;

pub mod cardano_block;
pub mod cardano_spent_txo;
pub mod cardano_transaction;
pub mod cardano_txo;
pub mod network;
mod serde_size;

pub trait Writer {
    type In;

    fn batch_copy(&self, data: &[Self::In]) -> impl Future<Output = anyhow::Result<()>> + Send;
}

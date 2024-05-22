use std::{future::Future, ops::Range};

use anyhow::Result;

pub type SlotRange = Range<u64>;

pub trait Recoverer {
    fn get_missing_slot_ranges(&mut self) -> impl Future<Output = Result<Vec<SlotRange>>> + Send;
    fn get_latest_slot(&mut self) -> impl Future<Output = Result<u64>> + Send;
}

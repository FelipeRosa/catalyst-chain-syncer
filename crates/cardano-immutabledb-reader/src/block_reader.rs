use std::{collections::BTreeSet, os::unix::fs::MetadataExt, path::Path, sync::Arc};

use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::dir_chunk_numbers;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockReaderConfig {
    pub worker_read_buffer_byte_size: usize,
    pub read_worker_count: usize,
    pub unprocessed_data_buffer_byte_size: usize,
}

impl Default for BlockReaderConfig {
    fn default() -> Self {
        Self {
            worker_read_buffer_byte_size: 8 * 1024 * 1024, // 8MB
            read_worker_count: 1,
            unprocessed_data_buffer_byte_size: 8 * 1024 * 1024, // 8MB
        }
    }
}

pub struct BlockReader {
    _cancellation_token: CancellationToken,
    read_data_rx: mpsc::UnboundedReceiver<(OwnedSemaphorePermit, Vec<u8>)>,
}

impl BlockReader {
    pub async fn new<P>(immutabledb_path: P, config: &BlockReaderConfig) -> anyhow::Result<Self>
    where
        P: AsRef<Path>,
    {
        let task_chunk_numbers = {
            let mut split_chunk_numbers = vec![(0u64, BTreeSet::new()); config.read_worker_count];

            let chunk_numbers = dir_chunk_numbers(immutabledb_path.as_ref()).await?;

            for chunk_number in chunk_numbers {
                let f = tokio::fs::File::open(
                    immutabledb_path
                        .as_ref()
                        .join(format!("{chunk_number:05}.chunk")),
                )
                .await?;
                let f_metadata = f.metadata().await?;

                let min = split_chunk_numbers
                    .iter_mut()
                    .min_by_key(|(byte_count, _)| *byte_count)
                    .expect("At least one entry");

                min.0 += f_metadata.size() as u64;
                min.1.insert(chunk_number);
            }

            split_chunk_numbers
        };

        let cancellation_token = CancellationToken::new();
        let (read_data_tx, read_data_rx) = mpsc::unbounded_channel();
        let read_semaphore = Arc::new(Semaphore::new(config.unprocessed_data_buffer_byte_size));

        for (_, chunk_numbers) in task_chunk_numbers {
            // Allocate a read buffer for each reader task
            let read_buffer = vec![0u8; config.worker_read_buffer_byte_size];

            tokio::spawn(chunk_reader_task::start(
                immutabledb_path.as_ref().to_path_buf(),
                read_buffer,
                chunk_numbers,
                read_semaphore.clone(),
                read_data_tx.clone(),
                cancellation_token.child_token(),
            ));
        }

        Ok(Self {
            _cancellation_token: cancellation_token,
            read_data_rx,
        })
    }

    pub async fn next(&mut self) -> Option<(OwnedSemaphorePermit, Vec<u8>)> {
        self.read_data_rx.recv().await
    }
}

mod chunk_reader_task {
    use std::{collections::BTreeSet, path::PathBuf, sync::Arc};

    use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
    use tokio_util::sync::CancellationToken;

    use crate::read_chunk_file;

    pub async fn start(
        immutabledb_path: PathBuf,
        mut read_buffer: Vec<u8>,
        mut chunk_numbers: BTreeSet<u32>,
        read_semaphore: Arc<Semaphore>,
        read_data_tx: mpsc::UnboundedSender<(OwnedSemaphorePermit, Vec<u8>)>,
        cancellation_token: CancellationToken,
    ) {
        'main_loop: while let Some(chunk_number) = chunk_numbers.pop_first() {
            let chunk_number_str = format!("{chunk_number:05}");

            let chunk_file_path = immutabledb_path.join(format!("{chunk_number_str}.chunk"));
            let secondary_file_path =
                immutabledb_path.join(format!("{chunk_number_str}.secondary"));

            let mut chunk_iter =
                read_chunk_file(chunk_file_path, secondary_file_path, &mut read_buffer)
                    .await
                    .expect("read_chunk_file");

            loop {
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        break;
                    }

                    res = chunk_iter.next() => {
                        match res {
                            Ok(Some(block_data)) => {
                                let permit = read_semaphore
                                    .clone()
                                    .acquire_many_owned(block_data.len() as u32)
                                    .await
                                    .expect("Acquire");

                                if read_data_tx.send((permit, block_data.to_vec())).is_err() {
                                    break 'main_loop;
                                }
                            }
                            Ok(None) => {
                                break;
                            }
                            Err(e) => panic!("{}", e),
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeSet;

    use pallas_traverse::MultiEraBlock;

    use super::{BlockReader, BlockReaderConfig};

    #[tokio::test]
    async fn test_block_reader() {
        let config = BlockReaderConfig::default();

        let mut block_reader = BlockReader::new("tests_data", &config)
            .await
            .expect("Block reader started");

        let mut block_numbers = BTreeSet::new();

        while let Some((_permit, block_data)) = block_reader.next().await {
            let decoded_block_data = MultiEraBlock::decode(&block_data).expect("Decoded");
            block_numbers.insert(decoded_block_data.number());
        }

        assert_eq!(block_numbers.len(), 9766);

        let mut last_block_number = block_numbers.pop_first().expect("At least one block");
        for block_number in block_numbers {
            assert!(block_number == last_block_number + 1);
            last_block_number = block_number;
        }
    }
}

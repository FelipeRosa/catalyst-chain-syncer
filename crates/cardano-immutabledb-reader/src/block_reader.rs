use std::{
    collections::BTreeSet, future::Future, os::unix::fs::MetadataExt, path::Path, sync::Arc,
};

use tokio::{sync::Semaphore, task::JoinError};
use tokio_util::sync::CancellationToken;

use crate::dir_chunk_numbers;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockReaderConfig {
    pub worker_read_buffer_bytes_size: usize,
    pub read_worker_count: usize,
    pub processing_worker_count: usize,
}

impl Default for BlockReaderConfig {
    fn default() -> Self {
        Self {
            worker_read_buffer_bytes_size: 8 * 1024 * 1024, // 8MB
            read_worker_count: 2,
            processing_worker_count: 2,
        }
    }
}

pub struct BlockReader {
    cancellation_token: CancellationToken,
    chunk_reader_tasks_joinset: tokio::task::JoinSet<()>,
}

impl BlockReader {
    pub async fn new<P, F, Fut>(
        immutabledb_path: P,
        config: &BlockReaderConfig,
        on_block_callback: F,
    ) -> anyhow::Result<Self>
    where
        P: AsRef<Path>,
        Fut: Future<Output = ()> + Send + 'static,
        F: (Fn(Vec<u8>) -> Fut) + Send + Sync + 'static,
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

        let shared_on_block_callback = Arc::new(on_block_callback);
        let cancellation_token = CancellationToken::new();
        let processing_semaphore = Arc::new(Semaphore::new(128 * 1024 * 1024));

        let mut chunk_reader_tasks_joinset = tokio::task::JoinSet::new();
        for (_, chunk_numbers) in task_chunk_numbers {
            // Allocate a read buffer for each reader task
            let read_buffer = vec![0u8; config.worker_read_buffer_bytes_size];

            chunk_reader_tasks_joinset.spawn(chunk_reader_task::start(
                immutabledb_path.as_ref().to_path_buf(),
                config.processing_worker_count,
                read_buffer,
                chunk_numbers,
                shared_on_block_callback.clone(),
                processing_semaphore.clone(),
                cancellation_token.child_token(),
            ));
        }

        Ok(Self {
            cancellation_token,
            chunk_reader_tasks_joinset,
        })
    }

    pub async fn close(self) -> Result<(), JoinError> {
        self.cancellation_token.cancel();
        self.await
    }
}

impl Future for BlockReader {
    type Output = Result<(), JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.chunk_reader_tasks_joinset.poll_join_next(cx) {
            std::task::Poll::Ready(Some(Ok(_))) => {
                cx.waker().wake_by_ref();
                std::task::Poll::Pending
            }
            std::task::Poll::Ready(Some(Err(err))) => std::task::Poll::Ready(Err(err)),
            std::task::Poll::Ready(None) => std::task::Poll::Ready(Ok(())),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

mod chunk_reader_task {
    use std::{collections::BTreeSet, future::Future, path::PathBuf, sync::Arc};

    use tokio::sync::{mpsc, Semaphore};
    use tokio_util::sync::CancellationToken;

    use crate::read_chunk_file;

    pub async fn start<F, Fut>(
        immutabledb_path: PathBuf,
        processing_workers_count: usize,
        mut read_buffer: Vec<u8>,
        mut chunk_numbers: BTreeSet<u32>,
        on_block_callback: Arc<F>,
        processing_semaphore: Arc<Semaphore>,
        cancellation_token: CancellationToken,
    ) where
        Fut: Future<Output = ()> + Send,
        F: (Fn(Vec<u8>) -> Fut) + Send + Sync + 'static,
    {
        let mut processing_worker_txs = Vec::new();
        let mut processing_workers_joinset = tokio::task::JoinSet::new();

        for _ in 0..processing_workers_count {
            let (processing_data_tx, mut processing_data_rx) =
                mpsc::unbounded_channel::<(_, Vec<_>)>();

            processing_workers_joinset.spawn({
                let worker_on_block_callback = on_block_callback.clone();
                let cancellation_token = cancellation_token.clone();

                async move {
                    loop {
                        tokio::select! {
                            _ = cancellation_token.cancelled() => {
                                break;
                            }

                            res = processing_data_rx.recv() => {
                                match res {
                                    Some((_permit, block_data)) => {
                                        worker_on_block_callback(block_data).await;
                                    }
                                    None => {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            });

            processing_worker_txs.push(processing_data_tx);
        }

        // Drop these since we've already moved clones to the processing worker threads.
        drop(on_block_callback);
        drop(cancellation_token);

        let mut i = 0;
        'main_loop: while let Some(chunk_number) = chunk_numbers.pop_first() {
            let chunk_number_str = format!("{chunk_number:05}");

            let chunk_file_path = immutabledb_path.join(format!("{chunk_number_str}.chunk"));
            let secondary_file_path =
                immutabledb_path.join(format!("{chunk_number_str}.secondary"));

            let mut chunk_iter =
                read_chunk_file(chunk_file_path, secondary_file_path, &mut read_buffer)
                    .await
                    .expect("read_chunk_file");

            while let Some(block_data) = chunk_iter.next().await.expect("chunk_iter.next") {
                let permit = processing_semaphore
                    .clone()
                    .acquire_many_owned(block_data.len() as u32);

                // if any worker fails we stop (for now)
                if processing_worker_txs[i]
                    .send((permit, block_data.to_vec()))
                    .is_err()
                {
                    break 'main_loop;
                }

                i = (i + 1) % processing_workers_count;
            }
        }

        drop(processing_worker_txs);

        while processing_workers_joinset.join_next().await.is_some() {}
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeSet;

    use pallas_traverse::MultiEraBlock;

    use super::{BlockReader, BlockReaderConfig};

    #[tokio::test]
    async fn test_block_reader() {
        let (block_numbers_tx, mut block_numbers_rx) = tokio::sync::mpsc::unbounded_channel();

        let config = BlockReaderConfig::default();

        let mut block_reader = BlockReader::new("tests_data", &config, move |block_data| {
            let block_numbers_tx = block_numbers_tx.clone();

            async move {
                let block = MultiEraBlock::decode(&block_data).expect("Decoded");
                let _ = block_numbers_tx.send(block.number());
            }
        })
        .await
        .expect("Block reader started");

        let mut done = false;
        let mut block_numbers = BTreeSet::new();

        loop {
            tokio::select! {
                res = block_numbers_rx.recv() => {
                    let Some(block_number) = res else {
                        break;
                    };

                    block_numbers.insert(block_number);
                }

                res = &mut block_reader, if !done => {
                    res.expect("All blocks read");
                    done = true;
                }
            }
        }

        assert_eq!(block_numbers.len(), 9766);

        let mut last_block_number = block_numbers.pop_first().expect("At least one block");
        for block_number in block_numbers {
            assert!(block_number == last_block_number + 1);
            last_block_number = block_number;
        }
    }
}

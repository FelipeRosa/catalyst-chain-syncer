use std::{
    collections::BTreeSet, error::Error, future::Future, os::unix::fs::MetadataExt, path::Path,
    sync::Arc,
};

use crate::dir_chunk_numbers;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockReaderConfig {
    pub worker_read_buffer_bytes_size: usize,
    pub read_worker_count: usize,
    pub processing_worker_count: usize,
    pub worker_processing_buffer_bytes_size: usize,
}

impl Default for BlockReaderConfig {
    fn default() -> Self {
        Self {
            worker_read_buffer_bytes_size: 8 * 1024 * 1024, // 8MB
            read_worker_count: 2,
            processing_worker_count: 2,
            worker_processing_buffer_bytes_size: 8 * 1024 * 1024, // 8MB
        }
    }
}

pub struct BlockReader {
    chunk_reader_tasks_joinset: tokio::task::JoinSet<()>,
}

impl BlockReader {
    pub async fn new<P, F>(
        immutabledb_path: P,
        config: &BlockReaderConfig,
        on_block_callback: F,
    ) -> Result<Self, Box<dyn Error>>
    where
        P: AsRef<Path>,
        F: Fn(&[u8]) + Send + Sync + 'static,
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

        let mut chunk_reader_tasks_joinset = tokio::task::JoinSet::new();
        for (_, chunk_numbers) in task_chunk_numbers {
            // Allocate a read buffer for each reader task
            let read_buffer = vec![0u8; config.worker_read_buffer_bytes_size];

            chunk_reader_tasks_joinset.spawn(chunk_reader_task::start(
                immutabledb_path.as_ref().to_path_buf(),
                config.processing_worker_count,
                config.worker_processing_buffer_bytes_size,
                read_buffer,
                chunk_numbers,
                shared_on_block_callback.clone(),
            ));
        }

        Ok(Self {
            chunk_reader_tasks_joinset,
        })
    }
}

impl Future for BlockReader {
    type Output = Result<(), tokio::task::JoinError>;

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
    use std::{collections::BTreeSet, path::PathBuf, sync::Arc};

    use tokio::sync::mpsc;

    use crate::read_chunk_file;

    pub async fn start<F>(
        immutabledb_path: PathBuf,
        processing_workers_count: usize,
        processing_buffer_size: usize,
        mut read_buffer: Vec<u8>,
        mut chunk_numbers: BTreeSet<u32>,
        on_block_callback: Arc<F>,
    ) where
        F: Fn(&[u8]) + Send + Sync + 'static,
    {
        let (prealloc_buffers_tx, mut prealloc_buffers_rx) = mpsc::unbounded_channel();

        // Use 80KB for each block buffer
        let n = processing_buffer_size / (80 * 1024);
        for _ in 0..n {
            prealloc_buffers_tx
                .send(vec![0u8; 80 * 1024])
                .expect("Sent");
        }

        let mut processing_worker_txs = Vec::new();
        let mut processing_workers_joinset = tokio::task::JoinSet::new();

        for _ in 0..processing_workers_count {
            let (processing_data_tx, mut processing_data_rx) = mpsc::unbounded_channel::<Vec<_>>();

            processing_workers_joinset.spawn_blocking({
                let worker_on_block_callback = on_block_callback.clone();
                let worker_prealloc_buffers_tx = prealloc_buffers_tx.clone();

                move || {
                    while let Some(block_data) = processing_data_rx.blocking_recv() {
                        worker_on_block_callback(&block_data);
                        worker_prealloc_buffers_tx.send(block_data).expect("Send");
                    }
                }
            });

            processing_worker_txs.push(processing_data_tx);
        }

        // Drop this since we've already passed references to it for the
        // processing worker threads.
        drop(on_block_callback);
        drop(prealloc_buffers_tx);

        let mut i = 0;
        while let Some(chunk_number) = chunk_numbers.pop_first() {
            let chunk_number_str = format!("{chunk_number:05}");

            let chunk_file_path = immutabledb_path.join(format!("{chunk_number_str}.chunk"));
            let secondary_file_path =
                immutabledb_path.join(format!("{chunk_number_str}.secondary"));

            let mut chunk_iter =
                read_chunk_file(chunk_file_path, secondary_file_path, &mut read_buffer)
                    .await
                    .expect("read_chunk_file");

            while let Some(block_data) = chunk_iter.next().await.expect("chunk_iter.next") {
                let mut prealloc_buffer = prealloc_buffers_rx.recv().await.expect("Recv");
                if prealloc_buffer.capacity() < block_data.len() {
                    prealloc_buffer.resize(block_data.len(), 0);
                }

                prealloc_buffer.resize(block_data.len(), 0);
                prealloc_buffer.copy_from_slice(block_data);

                if processing_worker_txs[i].send(prealloc_buffer).is_err() {
                    break;
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
            let block = MultiEraBlock::decode(block_data).expect("Decoded");
            let _ = block_numbers_tx.send(block.number());
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

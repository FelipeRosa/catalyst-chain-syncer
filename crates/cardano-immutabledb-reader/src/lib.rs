use std::{collections::BTreeSet, error::Error, ffi::OsStr, path::Path};

use binary_layout::binary_layout;
use tokio::io::AsyncReadExt;

pub async fn dir_chunk_numbers<P: AsRef<Path>>(path: P) -> Result<BTreeSet<u32>, Box<dyn Error>> {
    let mut dir = tokio::fs::read_dir(path).await?;
    let mut chunk_numbers = BTreeSet::new();

    while let Some(e) = dir.next_entry().await? {
        let entry_path = e.path();

        if let Some("chunk") = entry_path.extension().and_then(OsStr::to_str) {
            let Some(stem) = entry_path.file_stem().and_then(OsStr::to_str) else {
                continue;
            };

            chunk_numbers.insert(stem.parse()?);
        }
    }

    Ok(chunk_numbers)
}

binary_layout!(secondary_index_entry_layout, BigEndian, {
    block_offset: u64,
    header_offset: u16,
    header_size: u16,
    checksum: [u8; 4],
    header_hash: [u8; 32],
    block_or_ebb: u64,
});

const SECONDARY_INDEX_ENTRY_SIZE: usize = match secondary_index_entry_layout::SIZE {
    Some(size) => size,
    None => panic!("Expected secondary entry layout to have constant size"),
};

struct SecondaryIndexEntry {
    block_offset: u64,
}

struct SecondaryIndex {
    entries: Vec<SecondaryIndexEntry>,
}

impl SecondaryIndex {
    async fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn Error>> {
        let mut secondary_index_file = tokio::fs::File::open(path).await?;

        let mut entries = Vec::new();
        let mut entry_buf = [0u8; SECONDARY_INDEX_ENTRY_SIZE];

        loop {
            // Maybe this should use the sync version of sync_exact?
            if let Err(err) = secondary_index_file.read_exact(&mut entry_buf).await {
                if err.kind() == std::io::ErrorKind::UnexpectedEof {
                    break;
                }

                return Err(Box::new(err));
            }

            let view = secondary_index_entry_layout::View::new(&entry_buf);

            entries.push(SecondaryIndexEntry {
                block_offset: view.block_offset().read(),
            });
        }

        Ok(SecondaryIndex { entries })
    }
}

struct ReadChunkFile<'a> {
    secondary_index: SecondaryIndex,
    chunk_file_data: &'a mut Vec<u8>,
    counter: usize,
}

impl<'a> ReadChunkFile<'a> {
    async fn next(&mut self) -> Result<Option<&[u8]>, Box<dyn Error>> {
        let next_counter = self.counter + 1;

        let (from, to) = match next_counter.cmp(&self.secondary_index.entries.len()) {
            std::cmp::Ordering::Less => {
                let from = self.secondary_index.entries[self.counter].block_offset as usize;
                let to = self.secondary_index.entries[next_counter].block_offset as usize;

                (from, to)
            }
            std::cmp::Ordering::Equal => {
                let from = self.secondary_index.entries[self.counter].block_offset as usize;
                let to = self.chunk_file_data.len();

                (from, to)
            }
            std::cmp::Ordering::Greater => {
                return Ok(None);
            }
        };

        let data_slice = self.chunk_file_data.get(from..to);

        if data_slice.is_some() {
            self.counter = next_counter;
            Ok(data_slice)
        } else {
            Ok(None)
        }
    }
}

async fn read_chunk_file(
    path: impl AsRef<Path>,
    secondary_index_path: impl AsRef<Path>,
    data_buffer: &mut Vec<u8>,
) -> Result<ReadChunkFile, Box<dyn Error>> {
    let mut chunk_file = tokio::fs::File::open(path).await?;
    chunk_file.read_to_end(data_buffer).await?;

    let secondary_index = SecondaryIndex::from_file(secondary_index_path).await?;

    Ok(ReadChunkFile {
        secondary_index,
        chunk_file_data: data_buffer,
        counter: 0,
    })
}

pub struct ChunkReader {}

#[cfg(test)]
mod test {
    use std::collections::BTreeSet;

    use pallas_traverse::MultiEraBlock;

    use crate::{dir_chunk_numbers, read_chunk_file, SecondaryIndex};

    #[tokio::test]
    async fn test_dir_chunk_numbers() {
        let expected_chunk_numbers = (0..=12).collect::<BTreeSet<_>>();

        let chunk_numbers = dir_chunk_numbers("tests_data")
            .await
            .expect("Successfully read chunk numbers");

        assert_eq!(chunk_numbers, expected_chunk_numbers);
    }

    #[tokio::test]
    async fn test_secondary_index_from_file() {
        let index = SecondaryIndex::from_file("tests_data/00012.secondary")
            .await
            .expect("Successfully read secondary index file");

        assert_eq!(index.entries.len(), 1080);
    }

    #[tokio::test]
    async fn test_read_chunk_file() {
        let mut buffer = Vec::new();

        let mut iter = read_chunk_file(
            "tests_data/00012.chunk",
            "tests_data/00012.secondary",
            &mut buffer,
        )
        .await
        .expect("Chunk file iterator created");

        let mut last_block_no = None;
        let mut count = 0;
        while let Some(data) = iter
            .next()
            .await
            .expect("Successfully ready block data from chunk file")
        {
            // Make sure we are getting valid block data
            let block = MultiEraBlock::decode(data).expect("Valid block");

            if let Some(block_no) = last_block_no {
                assert!(block.number() > block_no);
                assert_eq!(block.number() - block_no, 1u64);
            }

            last_block_no = Some(block.number());
            count += 1;
        }

        assert_eq!(count, 1080);
    }
}

use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{BufWriter, Write},
    ops::Deref,
    path::PathBuf,
};

use async_trait::async_trait;
use fastcdc::v2020::FastCDC;

use crate::cdc::{
    cdc_config::{
        BUFFER_SIZE, CHUNK_AVG_SIZE, HASH_LENGTH, PACKS_DIR, SUPER_CHUNK_SIZE_MAX,
        SUPER_CHUNK_SIZE_MIN,
    },
    cdc_error::CdcResult,
    chunk_backend::{ChunkBackend, HashMapChunkBackend, PendingCdcChunk},
    manifest_backend::{CdcManifest, GitManifestBackend, ManifestBackend},
    pointer::{CdcPointer, CdcPointerBytes},
    utils::calculate_hash,
};

use memmap2::Mmap;
use rayon::iter::{IntoParallelRefIterator, ParallelDrainRange, ParallelIterator};

#[async_trait]
pub trait CdcStoreBackend: Send + Sync {
    async fn write_file(&self, file: File) -> CdcResult<CdcPointerBytes>;
    async fn read_file(&self, pointer: &CdcPointer, file: &File) -> CdcResult<usize>;
    fn gc(&self, keep_manifests: Vec<CdcPointer>) -> CdcResult<()>;
}

pub struct ChunkStoreBackend {
    chunk_backend: Box<dyn ChunkBackend>,
    manifest_backend: Box<dyn ManifestBackend>,
}

struct LastRead {
    pack_id: u32,
    read_offset: u32,
    read_length: u32,
}

impl ChunkStoreBackend {
    pub fn new(store_path: &PathBuf) -> CdcResult<Self> {
        let pack_dir = store_path.join(PACKS_DIR);
        if !pack_dir.exists() {
            std::fs::create_dir_all(&pack_dir)?;
        }

        let chunk_backend = HashMapChunkBackend::new(store_path)?;
        let manifest_backend = GitManifestBackend::new(store_path)?;

        Ok(Self {
            chunk_backend: Box::new(chunk_backend),
            manifest_backend: Box::new(manifest_backend),
        })
    }
}

#[async_trait]
impl CdcStoreBackend for ChunkStoreBackend {
    #[allow(unsafe_code)]
    async fn write_file(&self, file: File) -> CdcResult<CdcPointerBytes> {
        let mmap = unsafe { memmap2::MmapOptions::new().map(&file).unwrap() };

        let mut manifest: CdcManifest = Vec::new();

        let mut segments = mincdc::SliceChunker::new(
            &mmap,
            SUPER_CHUNK_SIZE_MIN,
            SUPER_CHUNK_SIZE_MAX,
            mincdc::MinCdcHash4::new(),
        )
        .collect::<Vec<mincdc::Chunk>>();

        let all_chunks: Vec<(usize, usize)> = segments
            .par_drain(..)
            .flat_map(|chunk| {
                let offset = chunk.offset();
                let segment = chunk.deref();
                let chunker = FastCDC::new(
                    segment,
                    CHUNK_AVG_SIZE / 4,
                    CHUNK_AVG_SIZE,
                    CHUNK_AVG_SIZE * 4,
                );
                chunker
                    .map(|new_chunk| {
                        (
                            offset + new_chunk.offset,
                            offset + new_chunk.offset + new_chunk.length,
                        )
                    })
                    .collect::<Vec<(usize, usize)>>()
            })
            .collect();

        for chunk in all_chunks.chunks(512) {
            let pending_chunks = chunk
                .par_iter()
                .map(|&(start, end)| {
                    let data = &mmap[start..end];
                    let hash = calculate_hash(data);
                    PendingCdcChunk {
                        hash: hash[0..HASH_LENGTH].try_into().unwrap(),
                        data: data,
                    }
                })
                .collect::<Vec<PendingCdcChunk>>();

            for pending_chunk in pending_chunks.iter() {
                manifest.push(pending_chunk.hash);
            }
            self.chunk_backend.write_chunks(pending_chunks)?;
        }

        let pointer = self.manifest_backend.write_manifest(manifest)?;
        self.chunk_backend.wait_for_write_finished()?;
        Ok(pointer)
    }

    async fn read_file(&self, pointer: &CdcPointer, file: &File) -> CdcResult<usize> {
        let manifest = self.manifest_backend.read_manifest(pointer)?;
        tracing::debug!("Manifest read: {:?} chunks", manifest);

        file.set_len(0)?;
        let mut output_writer = BufWriter::with_capacity(BUFFER_SIZE, file);
        let mut file_cache = HashMap::new();

        let mut last_read = LastRead {
            pack_id: 0,
            read_offset: 0,
            read_length: 0,
        };

        let mut file_size = 0usize;
        let locations = self.chunk_backend.read_chunk_location(manifest)?;
        tracing::debug!("locations read: {:?} chunks", locations);

        for location in locations {
            if !file_cache.contains_key(&location.pack_id) {
                file_cache.insert(
                    location.pack_id,
                    self.chunk_backend.read_chunk_file_mmap(location.pack_id)?,
                );
            }

            file_size += location.len_idx as usize + 1;

            // 定位数据块，检查能否和上次读取的块合并
            if last_read.pack_id == location.pack_id
                && last_read.read_offset + last_read.read_length == location.offset
            {
                // 可以合并读取
                last_read.read_length += location.len_idx as u32 + 1;
                continue;
            } else {
                // 不能合并读取，先读取上次的块，然后更新为当前块
                if let Some(mmap) = file_cache.get(&last_read.pack_id)
                    && last_read.read_length > 0
                {
                    // 读取上次的块
                    let length = last_read.read_length as usize;
                    write_file(
                        mmap,
                        &mut output_writer,
                        last_read.read_offset as usize,
                        length,
                    )?;
                }

                // 更新为当前块
                last_read.pack_id = location.pack_id;
                last_read.read_offset = location.offset;
                last_read.read_length = location.len_idx as u32 + 1;
            }
        }

        // 读取并写入最后一个块
        if let Some(mmap) = file_cache.get(&last_read.pack_id)
            && last_read.read_length > 0
        {
            // 读取上次的块
            let length = last_read.read_length as usize;
            write_file(
                mmap,
                &mut output_writer,
                last_read.read_offset as usize,
                length,
            )?;
        }

        output_writer.flush()?;

        Ok(file_size)
    }

    fn gc(&self, keep_manifests: Vec<CdcPointer>) -> CdcResult<()> {
        {
            let mut keep_chunks_hash = HashSet::new();
            for manifest in &keep_manifests {
                let manifest = self.manifest_backend.read_manifest(manifest)?;
                keep_chunks_hash.extend(manifest);
            }
            self.chunk_backend.gc(keep_chunks_hash)?;
        }

        self.manifest_backend.gc(&keep_manifests)?;
        Ok(())
    }
}

#[inline]
fn write_file(
    mmap: &Mmap,
    output_writer: &mut BufWriter<&File>,
    start_offset: usize,
    length: usize,
) -> CdcResult<()> {
    output_writer.write_all(&mmap[start_offset..start_offset + length])?;
    Ok(())
}

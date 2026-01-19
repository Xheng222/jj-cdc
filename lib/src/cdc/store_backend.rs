use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{BufWriter, Write},
    ops::Deref,
    path::PathBuf,
    sync::{self, Arc},
};

use fastcdc::v2020::FastCDC;

use crate::cdc::{
    cdc_config::{
        BUFFER_SIZE, CHUNK_AVG_SIZE, HASH_LENGTH, PACKS_DIR, SUPER_CHUNK_SIZE_MAX,
        SUPER_CHUNK_SIZE_MIN,
    },
    cdc_error::{CdcError, CdcResult},
    chunk_backend::{ChunkBackend, PendingCdcChunk, hashmap_backend::HashMapChunkBackend},
    manifest_backend::{CdcManifest, GitManifestBackend, ManifestBackend},
    pointer::{CdcPointer, CdcPointerBytes},
    utils::calculate_hash,
};

use memmap2::Mmap;
use rayon::iter::{IntoParallelRefIterator, ParallelDrainRange, ParallelIterator};

pub trait StoreBackend: Send + Sync {
    fn write_file(&self, file: File) -> CdcResult<CdcPointerBytes>;
    fn load_file(&self, pointer: &CdcPointer, file: &File) -> CdcResult<usize>;
    fn gc(&self, keep_manifests: Vec<CdcPointer>) -> CdcResult<()>;
}

pub struct ChunkStoreBackend {
    chunk_backend: Arc<std::sync::Mutex<Box<dyn ChunkBackend>>>,
    manifest_backend: Arc<std::sync::Mutex<Box<dyn ManifestBackend>>>,
    cdc_pool: rayon::ThreadPool,
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
        // let chunk_backend = RedbChunkBackend::new(store_path)?;
        let chunk_backend = HashMapChunkBackend::new(store_path)?;
        let manifest_backend = GitManifestBackend::new(store_path)?;

        let pool = rayon::ThreadPoolBuilder::new()
            .thread_name(|i| format!("cdc-{}", i))
            .build()
            .unwrap();
        Ok(Self {
            chunk_backend: Arc::new(std::sync::Mutex::new(Box::new(chunk_backend))),
            manifest_backend: Arc::new(std::sync::Mutex::new(Box::new(manifest_backend))),
            cdc_pool: pool,
        })
    }

    #[allow(unsafe_code)]
    fn _write_file(
        file: &File,
        chunk_backend: &mut Box<dyn ChunkBackend>,
        manifest_backend: &mut Box<dyn ManifestBackend>,
    ) -> CdcResult<CdcPointerBytes> {
        let mmap = unsafe { memmap2::MmapOptions::new().map(file).unwrap() };

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
            for pending_chunk in pending_chunks {
                manifest.push(pending_chunk.hash);
                chunk_backend.write_chunk_with_index(pending_chunk)?;
            }
        }
        let pointer = manifest_backend.write_manifest(manifest)?;
        chunk_backend.sync_writer()?;
        Ok(pointer)
    }
}

impl StoreBackend for ChunkStoreBackend {

    fn write_file(&self, file: File) -> CdcResult<CdcPointerBytes> {
        let chunk_backend = self.chunk_backend.clone();
        let manifest_backend = self.manifest_backend.clone();
        let (tx, rx) = sync::mpsc::sync_channel(0);

        self.cdc_pool.spawn(move || {
            let mut chunk_backend = chunk_backend.lock().unwrap();
            let mut manifest_backend = manifest_backend.lock().unwrap();

            let pointer = Self::_write_file(&file, &mut chunk_backend, &mut manifest_backend);
            tx.send(pointer).unwrap();
        });

        rx.recv()?
    }

    fn load_file(&self, pointer: &CdcPointer, file: &File) -> CdcResult<usize> {
        let manifest_backend = self.manifest_backend.lock().unwrap();
        let chunk_backend = self.chunk_backend.lock().unwrap();
        let manifest = manifest_backend.read_manifest(pointer)?;

        file.set_len(0)?;
        let mut output_writer = BufWriter::with_capacity(BUFFER_SIZE, file);
        let mut file_cache = HashMap::new();

        let mut last_read = LastRead {
            pack_id: 0,
            read_offset: 0,
            read_length: 0,
        };

        let mut file_size = 0usize;
        for hash in manifest {
            let location = match chunk_backend.read_index(&hash)? {
                Some(location) => location,
                None => return Err(CdcError::MissingChunk { hash: hash }),
            };

            if !file_cache.contains_key(&location.pack_id) {
                file_cache.insert(
                    location.pack_id,
                    chunk_backend.read_chunk_file_mmap(location.pack_id)?,
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
        let mut manifest_backend = self.manifest_backend.lock().unwrap();
        let mut chunk_backend = self.chunk_backend.lock().unwrap();


        {
            let mut keep_chunks_hash = HashSet::new();
            for manifest in &keep_manifests {
                let manifest = manifest_backend.read_manifest(manifest)?;
                keep_chunks_hash.extend(manifest);
            }
            chunk_backend.gc(keep_chunks_hash)?;
        }

        manifest_backend.gc(&keep_manifests)?;
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

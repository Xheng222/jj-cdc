use std::{
    collections::{HashMap, HashSet},
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::PathBuf,
    sync::{RwLock, mpsc},
};

use memmap2::Mmap;
use rayon::iter::{
    IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator,
    IntoParallelRefMutIterator, ParallelIterator,
};

use crate::cdc::{
    cdc_config::{
        BUFFER_SIZE, GLOBAL_LOCK, HASH_LENGTH, HASHMAP_INDEX_DIR, MAX_PACK_SIZE, PACKS_DIR,
        REPACK_THRESHOLD,
    },
    cdc_error::{CdcError, CdcResult},
    manifest_backend::CdcManifest,
};

#[derive(Clone, Debug)]
pub struct ChunkLocation {
    pub pack_id: u32, // 属于哪个 pack 文件
    pub offset: u32,  // 偏移量
    pub len_idx: u16, // 长度
}

pub struct PendingCdcChunk<'a> {
    pub hash: [u8; HASH_LENGTH],
    pub data: &'a [u8],
}

impl ChunkLocation {
    fn to_bytes(&self) -> [u8; 10] {
        let mut bytes = [0u8; 10];
        bytes[0..4].copy_from_slice(&self.pack_id.to_be_bytes());
        bytes[4..8].copy_from_slice(&self.offset.to_be_bytes());
        bytes[8..10].copy_from_slice(&self.len_idx.to_be_bytes());
        bytes
    }

    fn from_bytes(bytes: [u8; 10]) -> Self {
        Self {
            pack_id: u32::from_be_bytes(bytes[0..4].try_into().unwrap()),
            offset: u32::from_be_bytes(bytes[4..8].try_into().unwrap()),
            len_idx: u16::from_be_bytes(bytes[8..10].try_into().unwrap()),
        }
    }
}

pub trait ChunkBackend: Send + Sync {
    /// 写入单个 chunk
    fn _write_chunk(&self, pending_chunk: PendingCdcChunk) -> CdcResult<()>;

    /// 批量写入 chunk
    fn write_chunks(&self, pending_chunks: Vec<PendingCdcChunk>) -> CdcResult<()>;

    /// 等待所有写入完成，确保数据已经持久化
    fn wait_for_write_finished(&self) -> CdcResult<()>;

    /// 读取 chunk 文件的 mmap
    fn read_chunk_file_mmap(&self, pack_id: u32) -> CdcResult<Mmap>;

    /// 读取 chunk 位置
    fn read_chunk_location(&self, manifest: CdcManifest) -> CdcResult<Vec<ChunkLocation>>;

    /// GC，保留指定的 chunk，删除其他 chunk
    fn gc(&self, keep_chunks_hash: HashSet<[u8; HASH_LENGTH]>) -> CdcResult<()>;
}

enum ChunkWriterMessage {
    Write(ChunkLocation, Vec<u8>),
    Sync(mpsc::SyncSender<CdcResult<()>>),
}

pub struct ChunkWriter {
    current_pack_id: u32,
    current_pack_size: u32,
    chunk_dir_path: PathBuf,
    writer_lock_file: File,
    send_to_write: mpsc::Sender<ChunkWriterMessage>,
}

impl ChunkWriter {
    fn new(store_path: &PathBuf) -> CdcResult<Self> {
        let writer_lock_file_path = store_path.join(GLOBAL_LOCK);
        let mut file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&writer_lock_file_path)?;

        let mut bytes = Vec::new();
        file.lock()?;
        file.read_to_end(&mut bytes)?;

        let (current_chunk_file_id, current_chunk_file_size) = if bytes.len() >= 8 {
            let current_chunk_file_id =
                u32::from_be_bytes(bytes.get(0..4).unwrap().try_into().unwrap());
            let current_chunk_file_size =
                u32::from_be_bytes(bytes.get(4..8).unwrap().try_into().unwrap());
            (current_chunk_file_id, current_chunk_file_size)
        } else {
            (1, 0)
        };

        let (send_to_write, recv_tx) = mpsc::channel();

        let chunk_dir_path = store_path.join(PACKS_DIR);
        let chunk_dir_path_clone = store_path.join(PACKS_DIR);
        std::thread::spawn(move || {
            if let Err(e) = Self::backend_write_chunk(chunk_dir_path_clone, &recv_tx) {
                while let Ok(sync_tx) = recv_tx.recv() {
                    if let ChunkWriterMessage::Sync(sync_tx) = sync_tx {
                        sync_tx.send(Err(e)).ok();
                        break;
                    }
                }
            }
        });

        Ok(Self {
            current_pack_id: current_chunk_file_id,
            current_pack_size: current_chunk_file_size,
            chunk_dir_path: chunk_dir_path,
            writer_lock_file: file,
            send_to_write: send_to_write,
        })
    }

    #[inline]
    fn write_chunk(&mut self, data: &[u8]) -> CdcResult<ChunkLocation> {
        let data_len = data.len();
        let location = ChunkLocation {
            pack_id: self.current_pack_id,
            offset: self.current_pack_size,
            len_idx: (data_len - 1) as u16,
        };
        self.current_pack_size += data_len as u32;

        if self.current_pack_size >= MAX_PACK_SIZE {
            self.switch_new_chunk_file();
        }

        self.send_to_write
            .send(ChunkWriterMessage::Write(location.clone(), data.to_vec()))
            .map_err(CdcError::from_channel_sender)?;
        Ok(location)
    }

    fn wait_for_write_finished(&self) -> CdcResult<()> {
        let (sync_tx, sync_rx) = mpsc::sync_channel(0);
        self.send_to_write
            .send(ChunkWriterMessage::Sync(sync_tx))
            .map_err(CdcError::from_channel_sender)?;
        sync_rx.recv().map_err(CdcError::from_channel_sender)?
    }

    #[inline]
    fn switch_new_chunk_file(&mut self) {
        self.current_pack_id += 1;
        self.current_pack_size = 0;
    }

    #[allow(unsafe_code)]
    fn read_chunk_file_mmap(&self, pack_id: u32) -> CdcResult<Mmap> {
        let pack_path = self.chunk_dir_path.join(pack_id.to_string());
        let file = File::open(pack_path)?;
        let mmap = unsafe { memmap2::MmapOptions::new().map(&file)? };
        Ok(mmap)
    }

    fn backend_write_chunk(
        store_path: PathBuf,
        recv_tx: &mpsc::Receiver<ChunkWriterMessage>,
    ) -> CdcResult<()> {
        let mut current_pack_id = 0;
        let mut file_writer: Option<BufWriter<File>> = None;
        while let Ok(message) = recv_tx.recv() {
            match message {
                ChunkWriterMessage::Write(location, data) => {
                    if location.pack_id != current_pack_id {
                        current_pack_id = location.pack_id;
                        if let Some(mut file_writer) = file_writer.take() {
                            file_writer.flush()?;
                        }
                        let pack_path = store_path.join(&current_pack_id.to_string());
                        let new_file_writer = BufWriter::with_capacity(
                            BUFFER_SIZE,
                            fs::OpenOptions::new()
                                .create(true)
                                .write(true)
                                .append(true)
                                .open(&pack_path)?,
                        );

                        file_writer = Some(new_file_writer);
                    }

                    file_writer.as_mut().unwrap().write_all(&data)?;
                }
                ChunkWriterMessage::Sync(sync_tx) => {
                    if let Some(file_writer) = file_writer.as_mut() {
                        file_writer.flush()?;
                    }
                    sync_tx.send(Ok(())).ok();
                }
            }
        }

        Ok(())
    }

    #[inline]
    fn delete_pack(&self, pack_id: u32) {
        let pack_path = self.chunk_dir_path.join(pack_id.to_string());
        fs::remove_file(pack_path).ok();
    }

    #[inline]
    fn read_chunk_file_length(&self, pack_id: u32) -> CdcResult<u32> {
        let pack_path = self.chunk_dir_path.join(pack_id.to_string());
        let length = fs::metadata(pack_path)?.len() as u32;
        Ok(length)
    }

    fn save_writer(&mut self) -> CdcResult<()> {
        let mut writer_bytes = Vec::new();
        writer_bytes.extend_from_slice(&self.current_pack_id.to_be_bytes());
        writer_bytes.extend_from_slice(&self.current_pack_size.to_be_bytes());
        self.writer_lock_file.seek(SeekFrom::Start(0))?;
        self.writer_lock_file.write_all(&writer_bytes)?;
        self.writer_lock_file.flush()?;
        Ok(())
    }
}

impl Drop for ChunkWriter {
    fn drop(&mut self) {
        self.save_writer().ok();
    }
}

pub struct HashMapChunkBackend {
    index_buckets: RwLock<Vec<HashMap<[u8; HASH_LENGTH], ChunkLocation>>>,
    index_dir: PathBuf,
    chunk_writer: RwLock<ChunkWriter>,
}

impl HashMapChunkBackend {
    #[inline]
    fn get_bucket_index(hash: &[u8; HASH_LENGTH]) -> usize {
        (hash[0] >> 4) as usize
    }

    fn init_index_buckets(path: &PathBuf) -> CdcResult<HashMap<[u8; HASH_LENGTH], ChunkLocation>> {
        if !path.exists() {
            return Ok(HashMap::new());
        }
        const RECORD_SIZE: usize = HASH_LENGTH + 10;

        let file_size = fs::metadata(path)?.len() as usize;
        let estimated_records = file_size / RECORD_SIZE;
        let mut map = HashMap::with_capacity(estimated_records);

        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        let mut record = [0u8; RECORD_SIZE];
        loop {
            match reader.read_exact(&mut record) {
                Ok(_) => {
                    let mut key = [0u8; HASH_LENGTH];
                    let mut value = [0u8; 10];
                    key.copy_from_slice(&record[..HASH_LENGTH]);
                    value.copy_from_slice(&record[HASH_LENGTH..]);
                    map.insert(key, ChunkLocation::from_bytes(value));
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
        }

        Ok(map)
    }

    fn save_index_buckets(&mut self) -> CdcResult<()> {
        let results: Vec<_> = self
            .index_buckets
            .read()
            .unwrap()
            .par_iter()
            .enumerate()
            .map(|(i, bucket)| {
                let bucket_path = self.index_dir.join(format!("{:02x}.idx", i));
                let file = File::create(&bucket_path)?;
                let mut writer = BufWriter::new(file);
                for (key, value) in bucket.iter() {
                    writer.write_all(key)?;
                    writer.write_all(&value.to_bytes())?;
                }
                writer.flush()
            })
            .collect();

        for result in results {
            result?;
        }
        Ok(())
    }

    fn rewrite_pack(
        chunk_writer: &mut ChunkWriter,
        pack_id: u32,
        locations: Vec<&mut ChunkLocation>,
    ) -> CdcResult<()> {
        let pack_mmap = chunk_writer.read_chunk_file_mmap(pack_id)?;
        for location in locations {
            let offset = location.offset as usize;
            let len = location.len_idx as usize + 1;
            let data = &pack_mmap[offset..offset + len];
            let new_location = chunk_writer.write_chunk(data)?;
            location.pack_id = new_location.pack_id;
            location.offset = new_location.offset;
        }
        Ok(())
    }

    pub fn new(store_path: &PathBuf) -> CdcResult<Self> {
        let index_dir = store_path.join(HASHMAP_INDEX_DIR);
        if !index_dir.exists() {
            fs::create_dir_all(&index_dir)?;
        }

        let index_buckets = (0..16)
            .into_par_iter()
            .map(|i| {
                let bucket_path = index_dir.join(format!("{:02x}.idx", i));
                Self::init_index_buckets(&bucket_path).unwrap_or_default()
            })
            .collect();

        let chunk_writer = ChunkWriter::new(store_path)?;

        Ok(Self {
            index_buckets: RwLock::new(index_buckets),
            index_dir: index_dir,
            chunk_writer: RwLock::new(chunk_writer),
        })
    }
}

impl ChunkBackend for HashMapChunkBackend {
    fn _write_chunk(&self, pending_chunk: PendingCdcChunk) -> CdcResult<()> {
        let mut write_index = self.index_buckets.write().unwrap();
        let bucket_index = Self::get_bucket_index(&pending_chunk.hash);
        if !write_index[bucket_index].contains_key(&pending_chunk.hash) {
            let mut chunk_writer = self.chunk_writer.write().unwrap();
            let location = chunk_writer.write_chunk(pending_chunk.data)?;
            write_index[bucket_index].insert(pending_chunk.hash, location);
        }
        Ok(())
    }

    fn write_chunks(&self, pending_chunks: Vec<PendingCdcChunk>) -> CdcResult<()> {
        let mut write_index = self.index_buckets.write().unwrap();
        let mut chunk_writer = self.chunk_writer.write().unwrap();
        for pending_chunk in pending_chunks {
            let bucket_index = Self::get_bucket_index(&pending_chunk.hash);
            if !write_index[bucket_index].contains_key(&pending_chunk.hash) {
                let location = chunk_writer.write_chunk(pending_chunk.data)?;
                write_index[bucket_index].insert(pending_chunk.hash, location);
            }
        }

        Ok(())
    }

    fn wait_for_write_finished(&self) -> CdcResult<()> {
        self.chunk_writer.read().unwrap().wait_for_write_finished()
    }

    fn read_chunk_file_mmap(&self, pack_id: u32) -> CdcResult<Mmap> {
        self.chunk_writer
            .read()
            .unwrap()
            .read_chunk_file_mmap(pack_id)
    }

    fn read_chunk_location(&self, manifest: CdcManifest) -> CdcResult<Vec<ChunkLocation>> {
        let read_index = self.index_buckets.read().unwrap();
        let mut locations = Vec::new();
        for hash in manifest {
            let bucket_index = Self::get_bucket_index(&hash);
            if let Some(location) = read_index[bucket_index].get(&hash).cloned() {
                locations.push(location);
            } else {
                return Err(CdcError::MissingChunk { hash: hash });
            }
        }
        Ok(locations)
    }

    fn gc(&self, keep_chunks_hash: HashSet<[u8; HASH_LENGTH]>) -> CdcResult<()> {
        let mut write_index = self.index_buckets.write().unwrap();
        write_index.par_iter_mut().for_each(|bucket| {
            bucket.retain(|hash, _location| keep_chunks_hash.contains(hash));
        });

        let mut active_chunks: HashMap<u32, Vec<&mut ChunkLocation>> = write_index
            .par_iter_mut()
            .flat_map(|bucket| {
                bucket
                    .par_iter_mut()
                    .map(|(_hash, location)| (location.pack_id, location))
            })
            .fold(
                || HashMap::new(),
                |mut acc: HashMap<u32, Vec<&mut ChunkLocation>>, (pack_id, location)| {
                    acc.entry(pack_id).or_insert_with(Vec::new).push(location);
                    acc
                },
            )
            .reduce(HashMap::new, |mut acc, map| {
                for (pack_id, mut locations) in map {
                    acc.entry(pack_id)
                        .or_insert_with(Vec::new)
                        .append(&mut locations);
                }
                acc
            });

        // 先检查当前的 pack 文件是否要重写
        let mut chunk_writer = self.chunk_writer.write().unwrap();
        let mut keep_pack_ids = HashSet::new();
        let current_pack_id = chunk_writer.current_pack_id;

        if let Some(locations) = active_chunks.remove(&current_pack_id) {
            let length_sum = locations
                .par_iter()
                .map(|location| location.len_idx as u32)
                .sum::<u32>();
            let current_pack_length = chunk_writer.read_chunk_file_length(current_pack_id)?;
            if current_pack_length - length_sum > REPACK_THRESHOLD {
                chunk_writer.switch_new_chunk_file();
                Self::rewrite_pack(&mut chunk_writer, current_pack_id, locations)?;
            } else {
                // 当前这个 pack 有活跃数据，但是长度小于阈值，则保留
                keep_pack_ids.insert(current_pack_id);
            }
        } else {
            // 当前这个 pack 没有活跃数据，切换到新的并且准备删除
            chunk_writer.switch_new_chunk_file();
        }

        for (pack_id, locations) in active_chunks {
            let length_sum = locations
                .par_iter()
                .map(|location| location.len_idx as u32)
                .sum::<u32>();
            let current_pack_length = chunk_writer.read_chunk_file_length(pack_id)?;
            if current_pack_length - length_sum > REPACK_THRESHOLD {
                Self::rewrite_pack(&mut chunk_writer, pack_id, locations)?;
            } else {
                keep_pack_ids.insert(pack_id);
            }
        }

        chunk_writer.wait_for_write_finished()?;

        for pack_id in 1..=current_pack_id {
            if !keep_pack_ids.contains(&pack_id) {
                chunk_writer.delete_pack(pack_id);
            }
        }
        Ok(())
    }
}

impl Drop for HashMapChunkBackend {
    fn drop(&mut self) {
        self.save_index_buckets().ok();
    }
}

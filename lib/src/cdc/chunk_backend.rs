use std::collections::HashSet;

use memmap2::Mmap;

use crate::cdc::{cdc_config::HASH_LENGTH, cdc_error::CdcResult};

pub trait ChunkWriterBackend: Send {
    /// 写入 chunk
    fn write_chunk(&self, location: ChunkLocation, data: &[u8]) -> CdcResult<()>;

    /// 获取下一个 chunk 的位置
    fn get_next_chunk_index(&mut self, data_len: usize) -> ChunkLocation;

    /// 读取 chunk 文件的 mmap
    fn read_chunk_file_mmap(&self, pack_id: u32) -> CdcResult<Mmap>;

    fn sync_writer(&mut self) -> CdcResult<()>;
}

pub trait ChunkBackend: ChunkWriterBackend + Send {
    /// 写入 chunk 数据
    #[inline]
    fn write_chunk_with_index(&mut self, pending_chunk: PendingCdcChunk) -> CdcResult<()> {
        if self.contains_index(&pending_chunk.hash) {
            Ok(())
        } else {
            let location = self.get_next_chunk_index(pending_chunk.data.len());
            self.write_chunk(location.clone(), &pending_chunk.data)?;
            self.update_index(&pending_chunk.hash, location);
            Ok(())
        }
    }

    fn contains_index(&self, hash: &[u8; HASH_LENGTH]) -> bool;

    /// 获取 chunk 的位置
    fn read_index(&self, hash: &[u8; HASH_LENGTH]) -> CdcResult<Option<&ChunkLocation>>;

    fn _read_index_mut(
        &mut self,
        hash: &[u8; HASH_LENGTH],
    ) -> CdcResult<Option<&mut ChunkLocation>>;

    /// 更新 chunk 的位置
    /// - 如果 chunk 已经存在，则更新其位置
    /// - 如果 chunk 不存在，则添加
    fn update_index(&mut self, hash: &[u8; HASH_LENGTH], location: ChunkLocation);

    /// 移除 chunk 的位置
    // fn _remove(&mut self, hash: &[u8; HASH_LENGTH]) -> CdcResult<()>;
    fn gc(&mut self, keep_chunks_hash: HashSet<[u8; HASH_LENGTH]>) -> CdcResult<()>;
}

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

enum ChunkWriterMessage {
    Write(ChunkLocation, Vec<u8>),
    Sync,
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

mod chunk_writer {
    use crate::cdc::{
        cdc_config::{BUFFER_SIZE, GLOBAL_LOCK, MAX_PACK_SIZE, PACKS_DIR},
        cdc_error::{CdcError, CdcResult},
        chunk_backend::{ChunkLocation, ChunkWriterBackend, ChunkWriterMessage},
    };
    use memmap2::Mmap;
    use std::{
        fs::File,
        io::{BufWriter, Read, Seek, SeekFrom, Write},
        path::PathBuf,
        sync::mpsc,
    };

    pub struct ChunkWriter {
        pub(crate) pack_id: u32,
        current_size: u32,
        lock_file: File,
        store_path: PathBuf,
        send_tx: mpsc::Sender<ChunkWriterMessage>,
        sync_recv: mpsc::Receiver<CdcResult<()>>,
    }

    impl ChunkWriter {
        pub fn new(store_path: &PathBuf) -> CdcResult<Self> {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .read(true)
                .open(store_path.join(GLOBAL_LOCK))?;

            let mut bytes = Vec::new();
            file.lock()?;
            file.read_to_end(&mut bytes)?;

            let (pack_id, current_size) = if bytes.len() >= 8 {
                let pack_id = u32::from_be_bytes(bytes.get(0..4).unwrap().try_into().unwrap());
                let current_size = u32::from_be_bytes(bytes.get(4..8).unwrap().try_into().unwrap());
                (pack_id, current_size)
            } else {
                (1, 0)
            };

            let (send_tx, recv_tx) = mpsc::channel();
            let (sync_tx, sync_recv) = mpsc::sync_channel(0);

            let store_path_ = store_path.join(PACKS_DIR);
            std::thread::spawn(move || {
                if let Err(e) = Self::backend_write_chunk(store_path_, recv_tx, &sync_tx) {
                    sync_tx.send(Err(e)).ok();
                }
            });

            Ok(Self {
                pack_id: pack_id,
                current_size: current_size,
                lock_file: file,
                store_path: store_path.join(PACKS_DIR),
                send_tx: send_tx,
                sync_recv: sync_recv,
            })
        }

        fn to_bytes(&self) -> Vec<u8> {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&self.pack_id.to_be_bytes());
            bytes.extend_from_slice(&self.current_size.to_be_bytes());
            bytes
        }

        #[inline]
        pub(crate) fn switch_new_pack(&mut self) {
            self.pack_id += 1;
            self.current_size = 0;
        }

        #[inline]
        pub(crate) fn delete_pack(&self, pack_id: u32) {
            let pack_path = self.store_path.join(pack_id.to_string());
            std::fs::remove_file(pack_path).ok();
        }

        #[inline]
        pub(crate) fn read_chunk_file_length(&self, pack_id: u32) -> CdcResult<u32> {
            let pack_path = self.store_path.join(pack_id.to_string());
            let length = std::fs::metadata(pack_path)?.len() as u32;
            Ok(length)
        }

        fn save_writer(&mut self) -> CdcResult<()> {
            let writer_bytes = self.to_bytes();
            self.lock_file.seek(SeekFrom::Start(0))?;
            self.lock_file.write_all(&writer_bytes)?;
            self.lock_file.flush()?;
            Ok(())
        }

        fn backend_write_chunk(
            store_path: PathBuf,
            recv_tx: mpsc::Receiver<ChunkWriterMessage>,
            sync_tx: &mpsc::SyncSender<CdcResult<()>>,
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
                            let file_writer_ = BufWriter::with_capacity(
                                BUFFER_SIZE,
                                std::fs::OpenOptions::new()
                                    .create(true)
                                    .write(true)
                                    .append(true)
                                    .open(&pack_path)?,
                            );

                            file_writer = Some(file_writer_);
                        }

                        file_writer.as_mut().unwrap().write_all(&data)?;
                    }
                    ChunkWriterMessage::Sync => {
                        if let Some(file_writer) = file_writer.as_mut() {
                            file_writer.flush()?;
                        }
                        sync_tx.send(Ok(())).ok();
                    }
                }
            }

            Ok(())
        }
    }

    impl ChunkWriterBackend for ChunkWriter {
        #[inline]
        fn write_chunk(&self, location: ChunkLocation, data: &[u8]) -> CdcResult<()> {
            self.send_tx
                .send(ChunkWriterMessage::Write(location, data.to_vec()))
                .map_err(CdcError::from_channel_sender)
        }

        #[inline]
        fn get_next_chunk_index(&mut self, data_len: usize) -> ChunkLocation {
            let location = ChunkLocation {
                pack_id: self.pack_id,
                offset: self.current_size,
                len_idx: (data_len - 1) as u16,
            };
            self.current_size += data_len as u32;

            if self.current_size >= MAX_PACK_SIZE {
                self.switch_new_pack();
            }
            location
        }

        #[allow(unsafe_code)]
        fn read_chunk_file_mmap(&self, pack_id: u32) -> CdcResult<Mmap> {
            let pack_path = self.store_path.join(pack_id.to_string());
            let file = File::open(pack_path)?;
            let mmap = unsafe { memmap2::MmapOptions::new().map(&file)? };
            Ok(mmap)
        }

        fn sync_writer(&mut self) -> CdcResult<()> {
            self.send_tx
                .send(ChunkWriterMessage::Sync)
                .map_err(CdcError::from_channel_sender)?;
            self.sync_recv.recv()?
        }
    }

    impl Drop for ChunkWriter {
        fn drop(&mut self) {
            self.save_writer().ok();
        }
    }
}


pub mod hashmap_backend {
    use std::{
        collections::{HashMap, HashSet},
        fs::{self, File},
        io::{BufReader, BufWriter, Read, Write},
        path::PathBuf,
    };

    use crate::cdc::{
        cdc_config::{HASH_LENGTH, HASHMAP_INDEX_DIR, REPACK_THRESHOLD},
        cdc_error::CdcResult,
        chunk_backend::{
            ChunkBackend, ChunkLocation, ChunkWriterBackend, chunk_writer::ChunkWriter,
        },
    };
    use memmap2::Mmap;
    use rayon::iter::{
        IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator,
        IntoParallelRefMutIterator, ParallelIterator,
    };

    pub struct HashMapChunkBackend {
        index_buckets: Vec<HashMap<[u8; HASH_LENGTH], ChunkLocation>>,
        index_dir: PathBuf,
        chunk_writer: ChunkWriter,
    }

    impl HashMapChunkBackend {
        pub fn new(store_path: &PathBuf) -> CdcResult<Self> {
            let index_dir = store_path.join(HASHMAP_INDEX_DIR);
            let chunk_writer = ChunkWriter::new(store_path)?;
            if !index_dir.exists() {
                std::fs::create_dir_all(&index_dir)?;
            }

            let index_buckets = (0..16)
                .into_par_iter()
                .map(|i| {
                    let bucket_path = index_dir.join(format!("{:02x}.idx", i));
                    Self::init_index_buckets(&bucket_path).unwrap_or_default()
                })
                .collect();

            Ok(Self {
                index_buckets: index_buckets,
                index_dir: store_path.join(HASHMAP_INDEX_DIR),
                chunk_writer: chunk_writer,
            })
        }

        fn init_index_buckets(
            path: &PathBuf,
        ) -> CdcResult<HashMap<[u8; HASH_LENGTH], ChunkLocation>> {
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

        #[inline]
        fn get_bucket_index(hash: &[u8; HASH_LENGTH]) -> usize {
            (hash[0] >> 4) as usize
        }

        fn save_index_buckets(&mut self) -> CdcResult<()> {
            let results: Vec<_> = self
                .index_buckets
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
                let new_location = chunk_writer.get_next_chunk_index(len);
                location.pack_id = new_location.pack_id;
                location.offset = new_location.offset;
                chunk_writer.write_chunk(new_location, data)?;
            }
            Ok(())
        }
    }

    impl ChunkWriterBackend for HashMapChunkBackend {
        #[inline]
        fn write_chunk(&self, location: ChunkLocation, data: &[u8]) -> CdcResult<()> {
            self.chunk_writer.write_chunk(location, data)
        }

        #[inline]
        fn get_next_chunk_index(&mut self, data_len: usize) -> ChunkLocation {
            self.chunk_writer.get_next_chunk_index(data_len)
        }

        #[inline]
        fn read_chunk_file_mmap(&self, pack_id: u32) -> CdcResult<Mmap> {
            self.chunk_writer.read_chunk_file_mmap(pack_id)
        }

        #[inline]
        fn sync_writer(&mut self) -> CdcResult<()> {
            self.chunk_writer.sync_writer()
        }
    }

    impl ChunkBackend for HashMapChunkBackend {
        fn contains_index(&self, hash: &[u8; HASH_LENGTH]) -> bool {
            let bucket_index = Self::get_bucket_index(hash);
            self.index_buckets[bucket_index].contains_key(hash)
        }

        fn read_index(&self, hash: &[u8; HASH_LENGTH]) -> CdcResult<Option<&ChunkLocation>> {
            let bucket_index = Self::get_bucket_index(hash);
            Ok(self.index_buckets[bucket_index].get(hash))
        }

        fn _read_index_mut(
            &mut self,
            hash: &[u8; HASH_LENGTH],
        ) -> CdcResult<Option<&mut ChunkLocation>> {
            let bucket_index = Self::get_bucket_index(hash);
            Ok(self.index_buckets[bucket_index].get_mut(hash))
        }

        fn update_index(&mut self, hash: &[u8; HASH_LENGTH], location: ChunkLocation) {
            let bucket_index = Self::get_bucket_index(hash);
            self.index_buckets[bucket_index].insert(*hash, location);
        }

        fn gc(&mut self, keep_chunks_hash: HashSet<[u8; HASH_LENGTH]>) -> CdcResult<()> {
            self.index_buckets.par_iter_mut().for_each(|bucket| {
                bucket.retain(|hash, _location| keep_chunks_hash.contains(hash));
            });

            let mut active_chunks: HashMap<u32, Vec<&mut ChunkLocation>> = self
                .index_buckets
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
            let mut keep_pack_ids = HashSet::new();
            let current_pack_id = self.chunk_writer.pack_id;
            let chunk_writer = &mut self.chunk_writer;

            if let Some(locations) = active_chunks.remove(&current_pack_id) {
                let length_sum = locations
                    .par_iter()
                    .map(|location| location.len_idx as u32)
                    .sum::<u32>();
                let current_pack_length = chunk_writer.read_chunk_file_length(current_pack_id)?;
                if current_pack_length - length_sum > REPACK_THRESHOLD {
                    chunk_writer.switch_new_pack();
                    Self::rewrite_pack(chunk_writer, current_pack_id, locations)?;
                } else {
                    // 当前这个 pack 有活跃数据，但是长度小于阈值，则保留
                    keep_pack_ids.insert(current_pack_id);
                }
            } else {
                // 当前这个 pack 没有活跃数据，切换到新的并且准备删除
                chunk_writer.switch_new_pack();
            }

            for (pack_id, locations) in active_chunks {
                let length_sum = locations
                    .par_iter()
                    .map(|location| location.len_idx as u32)
                    .sum::<u32>();
                let current_pack_length = chunk_writer.read_chunk_file_length(pack_id)?;
                if current_pack_length - length_sum > REPACK_THRESHOLD {
                    Self::rewrite_pack(chunk_writer, pack_id, locations)?;
                } else {
                    keep_pack_ids.insert(pack_id);
                }
            }

            chunk_writer.sync_writer()?;
            // for pack_id in keep_pack_ids {
            //     chunk_writer.delete_pack(pack_id);
            // }
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
}

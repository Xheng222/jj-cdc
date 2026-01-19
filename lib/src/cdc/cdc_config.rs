// 常量
pub const MAX_BINARY_FILE_HEAD_SIZE: usize = 1024 * 8;
pub const LARGE_FILE_THRESHOLD: u64 = 1024 * 1024; // 1 MB
pub const MAX_PACK_SIZE: u32 = 256 * 1024 * 1024; // 256 MB
pub const BUFFER_SIZE: usize = 32 * 1024 * 1024; // 32 MB
pub const CHUNK_AVG_SIZE: u32 = 16 * 1024; // 16 KB

pub const SUPER_CHUNK_SIZE_MIN: usize = (1024.0 * 1024.0 * 512.0 * 0.75) as usize; // 384 MB
pub const SUPER_CHUNK_SIZE_MAX: usize = (1024.0 * 1024.0 * 512.0 * 1.25) as usize; // 640 MB

pub const REPACK_THRESHOLD: u32 = 32 * 1024 * 1024; // 碎片超过 32 MB 就要重写

pub const HASH_LENGTH: usize = 32;
pub const MAGIC: &[u8] = b"\x00CDC\x00"; // 二进制魔数
pub const MAGIC_LENGTH: usize = MAGIC.len();
pub const CDC_POINTER_SIZE: u64 = 45;

// 路径
pub const PACKS_DIR: &str = "packs";
pub const MANIFEST_GIT_DIR: &str = "manifest.git";
pub const HASHMAP_INDEX_DIR: &str = "index";
pub const GLOBAL_LOCK: &str = "global.lock";
// pub const CHUNK_DB: &str = "chunk.redb";
pub const MANIFEST_ANCHOR_REF: &str = "refs/manifests/anchor";

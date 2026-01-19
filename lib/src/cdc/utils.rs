use digest::{Digest, consts::U32};

/// 计算数据的 Hash
pub fn calculate_hash(data: &[u8]) -> [u8; 32] {
    let hash = blake2::Blake2b::<U32>::digest(data);
    hash.into()
}

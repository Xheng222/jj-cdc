#![expect(missing_docs)]

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::cdc::{
    cdc_config::{MAGIC, MAGIC_LENGTH},
    cdc_error::CdcResult,
};

pub type CdcPointerBytes = Vec<u8>;

#[derive(Serialize, Deserialize, Debug)]
pub struct CdcPointer {
    manifest_hash: String, // 指向 manifest 的 hash
}

/// try_parse 的结果类型
pub enum TryParseResult {
    /// 成功解析出 CDC 指针
    Parsed(CdcPointer),
    /// 不是 CDC 指针，返回已读取的字节（用于恢复原始数据流）
    NotCdcPointer(Vec<u8>),
}

impl CdcPointer {
    pub fn new(manifest_hash: String) -> Self {
        Self { manifest_hash }
    }

    pub fn hash(&self) -> &String {
        &self.manifest_hash
    }

    // 序列化 CDC 指针，返回 CdcPointerBytes
    pub fn serialize(self) -> CdcPointerBytes {
        let mut buf = Vec::new();
        buf.extend_from_slice(MAGIC);
        buf.extend_from_slice(self.manifest_hash.as_bytes());
        buf
    }

    /// 尝试解析 CDC 指针
    ///
    /// # 返回值
    /// - `Ok(TryParseResult::Parsed(pointer))` - 成功解析出 CDC 指针
    /// - `Ok(TryParseResult::NotCdcPointer(bytes))` - 不是 CDC 指针，返回已读取的字节
    /// - `Err(e)` - 是 CDC 指针但解析失败，或读取错误
    pub async fn try_parse<R: AsyncRead + Unpin>(reader: &mut R) -> CdcResult<TryParseResult> {
        // 尝试读取魔数
        let mut magic_buf = [0; MAGIC_LENGTH];
        let n = reader.read(&mut magic_buf).await?;
        if !Self::is_cdc_pointer(&magic_buf) {
            return Ok(TryParseResult::NotCdcPointer(magic_buf[..n].to_vec()));
        }

        // 魔数匹配，这应该是一个 CDC 指针
        let mut hash_bytes = Vec::new();
        reader.read_to_end(&mut hash_bytes).await?;

        // 解析 hash
        let hash = String::from_utf8(hash_bytes).unwrap();

        Ok(TryParseResult::Parsed(Self::new(hash)))
    }

    pub fn try_parse_from_bytes(bytes: &[u8]) -> Option<CdcPointer> {
        if Self::is_cdc_pointer(bytes) {
            let hash = String::from_utf8(bytes[MAGIC_LENGTH..].to_vec()).unwrap();
            Some(Self::new(hash))
        } else {
            None
        }
    }

    #[inline]
    pub fn is_cdc_pointer(bytes: &[u8]) -> bool {
        let is_start_with_magic = bytes.starts_with(MAGIC);
        tracing::debug!("is_cdc_pointer: starts_with_magic={}, bytes={:?}", is_start_with_magic, bytes);
        is_start_with_magic
    }
}

use std::fs::File;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write as _;

use digest::Digest;
use digest::consts::U32;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt as _;

use crate::cdc::backend_wrapper::CdcBackendWrapper;
use crate::cdc::cdc_config::MAGIC_LENGTH;
use crate::cdc::cdc_config::MAX_BINARY_FILE_HEAD_SIZE;
use crate::cdc::cdc_error::CdcResult;
use crate::cdc::pointer::CdcPointer;

/// 计算数据的 Hash
pub fn calculate_hash(data: &[u8]) -> [u8; 32] {
    let hash = blake2::Blake2b::<U32>::digest(data);
    hash.into()
}

/// 判断文件是否为二进制文件
pub fn is_binary_file(file: &mut File) -> bool {
    // 读取头部
    let mut buffer = [0u8; MAX_BINARY_FILE_HEAD_SIZE];
    let n = match file.read(&mut buffer) {
        Ok(n) => n,
        Err(_) => return false,
    };

    // 重置文件指针到开头
    match file.seek(SeekFrom::Start(0)) {
        Ok(_) => (),
        Err(_) => return true,
    }

    // 如果文件为空，认为它不是二进制文件
    if n == 0 {
        return false;
    }

    // 如果最后一个字节是 \r, 则去掉
    let bytes = if buffer.get(n - 1) == Some(&b'\r') {
        &buffer[0..n - 1]
    } else {
        &buffer[0..n]
    };

    // 判断是否包含 \0 或者 \r 后面不是 \n
    let mut bytes = bytes.iter().peekable();
    while let Some(byte) = bytes.next() {
        match *byte {
            b'\0' => return true,
            b'\r' if bytes.peek() != Some(&&b'\n') => {
                return true;
            }
            _ => {}
        }
    }
    return false;
}

/// 从 reader 中读取数据，如果是 CDC 指针则解析出指针信息，否则将数据原样写入 file
pub async fn write_file_maybe_from_pointer<R: AsyncRead + Unpin>(cdc_backend: &CdcBackendWrapper, file: &mut File, reader: &mut R) -> CdcResult<()> {
    let mut magic_buf = [0; MAGIC_LENGTH];
    reader.read(&mut magic_buf).await?;
    if CdcPointer::is_cdc_pointer(&magic_buf) {
        // 解析 CDC 指针
        let mut hash_bytes = Vec::new();
        reader.read_to_end(&mut hash_bytes).await?;

        // 解析 hash
        let hash = String::from_utf8(hash_bytes).unwrap();
        let pointer = CdcPointer::new(hash);

        cdc_backend.read_file_from_cdc(&pointer, file).await?;
    }
    else {
        // 不是 CDC 指针，将已读取的字节写入文件
        file.write_all(&magic_buf).unwrap();
        // 将剩余数据写入文件
        let mut buf = [0; 1024];
        loop {
            let n = reader.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            file.write_all(&buf[..n]).unwrap();
        }
    }

    Ok(())
}

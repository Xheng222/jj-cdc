use std::fs::File;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;

use digest::Digest;
use digest::consts::U32;

use crate::cdc::cdc_config::MAX_BINARY_FILE_HEAD_SIZE;

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

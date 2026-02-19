use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
};

use digest::{Digest, consts::U32};

use crate::cdc::cdc_config::{LARGE_FILE_THRESHOLD, MAX_BINARY_FILE_HEAD_SIZE};

/// 计算数据的 Hash
pub fn calculate_hash(data: &[u8]) -> [u8; 32] {
    let hash = blake2::Blake2b::<U32>::digest(data);
    hash.into()
}

/// 判断文件是否为二进制文件
pub fn is_binary_file(file: &mut File) -> bool {
    // 如果大于 1M，直接视为二进制
    if let Ok(meta) = file.metadata() {
        if meta.len() > LARGE_FILE_THRESHOLD {
            return true;
        }
    }

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

    let buffer = &buffer[..n];

    // - 检查 NULL 字节
    if buffer.contains(&0) {
        return true;
    }

    // - 检查 UTF-8 编码
    if String::from_utf8(buffer.to_vec()).is_ok() {
        return false;
    }

    // - 检查 UTF-16 LE 或 UTF-16 BE 编码
    if n > 2 && (buffer.starts_with(&[0xFF, 0xFE]) || buffer.starts_with(&[0xFE, 0xFF])) {
        return false;
    }

    // - 统计可打印 ASCII 字符
    // let non_ascii_count = buffer.iter().filter(|&b| *b > 127).count();
    let text_ascii_count = buffer
        .iter()
        .filter(|&b| {
            (*b >= 0x20 && *b <= 0x7E)
                || *b == 0x09
                || *b == 0x0A
                || *b == 0x0D
                || *b == 0x0C
                || *b == 0x08
        })
        .count();

    // 如果可打印 ASCII 字符占比超过 70%，则认为是二进制文件
    let text_ascii_ratio = text_ascii_count as f64 / n as f64;
    if text_ascii_ratio > 0.7 {
        return false;
    }
    return true;
}

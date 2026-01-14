//! # blockbucket
//!
//! A tiny file-backed key-value bucket (binary `Vec<u8>` key/value) with simple operations.
//!
//! **Supported operations**
//! - `set` / `get` / `delete`
//! - `set_many`
//! - `list` / `list_next` (pagination)
//! - `find_next`
//! - `delete_to`
//! - `list_lock_delete` (queue-like pop)
//!
//! The storage is backed by a **single file** (example: `data.db`).
//!
//! > ⚠️ Note: This crate is intentionally simple.
//! > It is not a transactional database and does not guarantee strong crash-safety or durability.
//!
//! ## Quick start
//! ```no_run
//! use blockbucket::{Bucket, Trait};
//!
//! fn main() -> std::io::Result<()> {
//!     let mut bucket = Bucket::new("data.db".to_string())?;
//!
//!     let key = b"test-key-001".to_vec();
//!     let value = b"hello blockbucket".to_vec();
//!
//!     bucket.set(key.clone(), value.clone())?;
//!
//!     let (k, v) = bucket.get(key.clone());
//!     assert_eq!(k, key);
//!     assert_eq!(v, value);
//!
//!     bucket.delete(key)?;
//!     Ok(())
//! }
//! ```
//!
//! ## Queue-like usage
//! ```no_run
//! use blockbucket::{Bucket, Trait};
//!
//! fn main() -> std::io::Result<()> {
//!     let mut bucket = Bucket::new("data.db".to_string())?;
//!     let popped = bucket.list_lock_delete(10)?;
//!     println!("popped={}", popped.len());
//!     Ok(())
//! }
//! ```
//!
//! ## Behavior notes
//! - `get(key)` returns `(Vec::new(), Vec::new())` if the key does not exist.
//! - Keys and values are stored as raw bytes (`Vec<u8>`).
//!

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Result, Seek, SeekFrom::Start, Write};

/// Public API implemented by [`Bucket`].
///
/// The API is intentionally minimal and uses raw bytes for keys and values.
pub trait Trait {
    /// Open a bucket at `path`.
    ///
    /// Creates the file if it doesn't exist.
    fn new(path: String) -> Result<Self>
    where
        Self: Sized;

    /// Insert or update a key/value pair.
    fn set(&mut self, key: Vec<u8>, data: Vec<u8>) -> Result<()>;

    /// Get a value by key.
    ///
    /// Returns `(Vec::new(), Vec::new())` if the key is not found.
    fn get(&mut self, key: Vec<u8>) -> (Vec<u8>, Vec<u8>);

    /// Delete an entry by key.
    fn delete(&mut self, key: Vec<u8>) -> Result<()>;

    /// Insert multiple items in one call.
    fn set_many(&mut self, list_data: Vec<(Vec<u8>, Vec<u8>)>) -> Result<()>;

    /// List up to `limit` items.
    fn list(&mut self, limit: u8) -> Vec<(Vec<u8>, Vec<u8>)>;

    /// Pagination helper: skip `skip` items and return up to `limit` items.
    fn list_next(&mut self, limit: u8, skip: usize) -> Vec<(Vec<u8>, Vec<u8>)>;

    /// Find a window of items around `key`.
    ///
    /// - `only_after_key = false`: include the found key (if exists)
    /// - `only_after_key = true`: return items after the found key
    fn find_next(
        &mut self,
        key: Vec<u8>,
        limit: u8,
        only_after_key: bool,
    ) -> Vec<(Vec<u8>, Vec<u8>)>;

    /// Delete items up to a key.
    ///
    /// - `also_delete_the_found_block = true`: include the found key in deletion
    /// - `also_delete_the_found_block = false`: keep the found key and delete items before it
    fn delete_to(&mut self, key: Vec<u8>, also_delete_the_found_block: bool) -> Result<()>;

    /// Read up to `limit` items and delete them (queue-like).
    fn list_lock_delete(&mut self, limit: u8) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
}

/// File-backed bucket storage.
///
/// This struct keeps **two independent file handles**:
/// - `reader`: used for read/scan operations (list, get, find_next…)
/// - `writer`: used for write operations (set, delete…)
///
/// Having separate handles helps avoid seek conflicts when reading and writing
/// in the same process.
///
/// This design makes it easier to:
/// - read sequentially while writes happen
/// - avoid frequent seek jumps on a single handle
/// - keep the code simple (no locking strategy inside the crate)
pub struct Bucket {
    /// File handle for read operations (scan / list / get).
    pub(crate) reader: File,

    /// File handle for write operations (append / update / delete).
    pub(crate) writer: File,
}

const MAX_DIGIT_GROUP: u8 = 249;
const START: u8 = 250;
const SIZE_KEY: u8 = 251;
const SUM_KEY: u8 = 252;
const SUM_MD5: u8 = 253;
const SIZE_DATA: u8 = 254;
const END: u8 = 255;
const FIRST_SIZE: usize = 128;

#[derive(Clone)]
struct Block {
    pub start: usize,
    pub size_key: usize,
    pub sum_key: usize,
    pub sum_md5: usize,
    pub size_data: usize,
}

const EMPTY_BLOCK: Block = Block {
    start: 0,
    size_key: 0,
    sum_key: 0,
    sum_md5: 0,
    size_data: 0,
};

fn group_digits_to_vec(mut n: usize) -> Vec<u8> {
    let mut digits = Vec::new();
    while n > 0 {
        digits.push((n % 10) as u8);
        n /= 10;
    }
    digits.reverse();

    let mut result = Vec::new();
    let mut i = 0;

    while i < digits.len() {
        // nếu chữ số hiện tại là 0 -> push 0 và tiếp tục
        if digits[i] == 0 {
            result.push(0);
            i += 1;
            continue;
        }

        // thử gom 3 chữ số
        if i + 2 < digits.len() {
            let v = (digits[i] as u16) * 100 + (digits[i + 1] as u16) * 10 + (digits[i + 2] as u16);

            if v <= MAX_DIGIT_GROUP as u16 {
                result.push(v as u8);
                i += 3;
                continue;
            }
        }

        // thử gom 2 chữ số
        if i + 1 < digits.len() {
            let v = digits[i] * 10 + digits[i + 1];
            if v <= MAX_DIGIT_GROUP {
                result.push(v);
                i += 2;
                continue;
            }
        }

        // fallback: 1 chữ số
        result.push(digits[i]);
        i += 1;
    }

    result
}
#[cfg(test)]
mod test_group_digits_to_vec {
    use crate::group_digits_to_vec;
    #[test]
    fn test_group_digits_to_vec() {
        let data = group_digits_to_vec(2502510011110001111);
        assert_eq!(data, [25u8, 0, 25, 100, 111, 100, 0, 111, 1]);
    }
}

fn digits_to_number(digits: &[u8]) -> usize {
    let mut n: usize = 0;
    for &x in digits {
        let count = if x < 10 {
            1
        } else if x < 100 {
            2
        } else {
            3
        };
        n = n * 10usize.pow(count) + x as usize;
    }
    n
}
#[cfg(test)]
mod test_digits_to_number {
    use crate::digits_to_number;
    #[test]
    fn test_digits_to_number() {
        let data = digits_to_number(&[250u8, 25, 100, 111, 100, 0, 111, 1]);
        assert_eq!(data, 2502510011110001111);
        let data = digits_to_number(&[0u8, 250, 25, 100, 111, 100, 0, 111, 1]);
        assert_eq!(data, 2502510011110001111);
    }
}

fn merge_vec(vec: &[Vec<u8>]) -> Vec<u8> {
    let total: usize = vec.iter().map(|v| v.len()).sum();
    let mut out = Vec::with_capacity(total);
    for v in vec {
        out.extend_from_slice(v);
    }
    out
}
#[cfg(test)]
mod test_merge_vec {
    use crate::merge_vec;
    #[test]
    fn test_merge_vec() {
        let data = merge_vec(&[vec![8u8, 9, 66, 4], vec![1u8, 1, 11, 1]]);
        assert_eq!(data, vec![8u8, 9, 66, 4, 1, 1, 11, 1]);
    }
}

fn pull_key(read: &mut File, info: &Block) -> Result<Vec<u8>> {
    read.seek(Start(info.start as u64))?;
    let mut found_key = vec![0u8; info.size_key];
    read.read_exact(&mut found_key)?;
    Ok(found_key)
}

fn pull_data(read: &mut File, info: &Block) -> Result<(Vec<u8>, Vec<u8>)> {
    read.seek(Start(info.start as u64))?;
    let mut found_key = vec![0u8; info.size_key];
    read.read_exact(&mut found_key)?;
    let mut found_data = vec![0u8; info.size_data];
    read.read_exact(&mut found_data)?;
    Ok((found_key, found_data))
}

fn get_one_data(read: &mut File, list_block_data: Vec<u8>, key: Vec<u8>) -> (Vec<u8>, Vec<u8>) {
    let mut result: (Vec<u8>, Vec<u8>) = (Vec::new(), Vec::new());
    {
        let len_key = key.len();
        let sum_key: usize = key.iter().map(|&x| x as usize).sum();
        let sum_md5: usize = md5::compute(&key)
            .to_vec()
            .iter()
            .map(|&x| x as usize)
            .sum();

        let mut block_info = EMPTY_BLOCK;
        let mut tmp_group: Vec<u8> = Vec::new();
        for v in list_block_data {
            match v {
                START => {
                    block_info.start = digits_to_number(&tmp_group);
                    tmp_group.clear();
                }
                SIZE_KEY => {
                    block_info.size_key = digits_to_number(&tmp_group);
                    tmp_group.clear();
                }
                SUM_KEY => {
                    block_info.sum_key = digits_to_number(&tmp_group);
                    tmp_group.clear();
                }
                SUM_MD5 => {
                    block_info.sum_md5 = digits_to_number(&tmp_group);
                    tmp_group.clear();
                }
                SIZE_DATA => {
                    block_info.size_data = digits_to_number(&tmp_group);
                    tmp_group.clear();
                    {
                        if block_info.size_key == len_key
                            && block_info.sum_key == sum_key
                            && block_info.sum_md5 == sum_md5
                        {
                            let (found_key, found_data) = pull_data(read, &block_info)
                                .unwrap_or_else(|_| (Vec::new(), Vec::new()));
                            if found_key == key {
                                // success
                                result.0 = found_key;
                                result.1 = found_data;
                                break;
                            }
                        }
                    }
                    block_info = EMPTY_BLOCK;
                }
                END => {
                    break;
                }
                _ => {
                    tmp_group.push(v);
                }
            }
        }
    }
    result
}

fn get_list_data(read: &mut File, list_block_data: Vec<u8>, limit: u8) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut result: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    {
        let mut block_info = EMPTY_BLOCK;
        let mut tmp_group: Vec<u8> = Vec::new();
        let mut current: u8 = 0;
        for v in list_block_data {
            if current >= limit {
                break;
            }
            match v {
                START => {
                    block_info.start = digits_to_number(&tmp_group);
                    tmp_group.clear();
                }
                SIZE_KEY => {
                    block_info.size_key = digits_to_number(&tmp_group);
                    tmp_group.clear();
                }
                SUM_KEY => {
                    block_info.sum_key = digits_to_number(&tmp_group);
                    tmp_group.clear();
                }
                SUM_MD5 => {
                    block_info.sum_md5 = digits_to_number(&tmp_group);
                    tmp_group.clear();
                }
                SIZE_DATA => {
                    block_info.size_data = digits_to_number(&tmp_group);
                    tmp_group.clear();
                    {
                        let (found_key, found_data) = pull_data(read, &block_info)
                            .unwrap_or_else(|_| (Vec::new(), Vec::new()));
                        let len_found_key = found_key.len();
                        if len_found_key == block_info.size_key {
                            let sum_found_key: usize = found_key.iter().map(|&x| x as usize).sum();
                            if sum_found_key == block_info.sum_key {
                                let sum_found_md5: usize = md5::compute(&found_key)
                                    .to_vec()
                                    .iter()
                                    .map(|&x| x as usize)
                                    .sum();
                                if sum_found_md5 == block_info.sum_md5 {
                                    // success
                                    result.push((found_key, found_data));
                                    current += 1;
                                }
                            }
                        }
                    }
                    block_info = EMPTY_BLOCK;
                }
                END => {
                    break;
                }
                _ => {
                    tmp_group.push(v);
                }
            }
        }
    }
    result
}

fn get_list_lock_delete_data(
    read: &mut File,
    write: &mut File,
    start_list_point: usize,
    list_block_data: Vec<u8>,
    limit: u8,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut result: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut end_key: Vec<u8> = Vec::new();
    {
        let mut block_info = EMPTY_BLOCK;
        let mut tmp_group: Vec<u8> = Vec::new();
        let mut current: u8 = 0;
        for v in list_block_data.clone() {
            if current >= limit {
                break;
            }
            match v {
                START => {
                    block_info.start = digits_to_number(&tmp_group);
                    tmp_group.clear();
                }
                SIZE_KEY => {
                    block_info.size_key = digits_to_number(&tmp_group);
                    tmp_group.clear();
                }
                SUM_KEY => {
                    block_info.sum_key = digits_to_number(&tmp_group);
                    tmp_group.clear();
                }
                SUM_MD5 => {
                    block_info.sum_md5 = digits_to_number(&tmp_group);
                    tmp_group.clear();
                }
                SIZE_DATA => {
                    block_info.size_data = digits_to_number(&tmp_group);
                    tmp_group.clear();
                    {
                        let (found_key, found_data) = pull_data(read, &block_info)
                            .unwrap_or_else(|_| (Vec::new(), Vec::new()));
                        let len_found_key = found_key.len();
                        if len_found_key == block_info.size_key {
                            let sum_found_key: usize = found_key.iter().map(|&x| x as usize).sum();
                            if sum_found_key == block_info.sum_key {
                                let sum_found_md5: usize = md5::compute(&found_key)
                                    .to_vec()
                                    .iter()
                                    .map(|&x| x as usize)
                                    .sum();
                                if sum_found_md5 == block_info.sum_md5 {
                                    // success
                                    end_key = found_key.clone();
                                    result.push((found_key, found_data));
                                    current += 1;
                                }
                            }
                        }
                    }
                    block_info = EMPTY_BLOCK;
                }
                END => {
                    break;
                }
                _ => {
                    tmp_group.push(v);
                }
            }
        }
    }
    delete_to_data(
        read,
        write,
        start_list_point,
        list_block_data,
        true,
        end_key,
    )?;
    Ok(result)
}

fn get_list_next_data(
    read: &mut File,
    list_block_data: Vec<u8>,
    limit: u8,
    skip: usize,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut result: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    {
        let mut block_info = EMPTY_BLOCK;
        let mut tmp_group: Vec<u8> = Vec::new();
        let mut current: u8 = 0;
        let mut current_skip: usize = 0;
        for v in list_block_data {
            if current >= limit {
                break;
            }
            match v {
                START => {
                    block_info.start = digits_to_number(&tmp_group);
                    tmp_group.clear();
                }
                SIZE_KEY => {
                    block_info.size_key = digits_to_number(&tmp_group);
                    tmp_group.clear();
                }
                SUM_KEY => {
                    block_info.sum_key = digits_to_number(&tmp_group);
                    tmp_group.clear();
                }
                SUM_MD5 => {
                    block_info.sum_md5 = digits_to_number(&tmp_group);
                    tmp_group.clear();
                }
                SIZE_DATA => {
                    block_info.size_data = digits_to_number(&tmp_group);
                    tmp_group.clear();
                    {
                        let (found_key, found_data) = pull_data(read, &block_info)
                            .unwrap_or_else(|_| (Vec::new(), Vec::new()));
                        let len_found_key = found_key.len();
                        if len_found_key == block_info.size_key {
                            let sum_found_key: usize = found_key.iter().map(|&x| x as usize).sum();
                            if sum_found_key == block_info.sum_key {
                                let sum_found_md5: usize = md5::compute(&found_key)
                                    .to_vec()
                                    .iter()
                                    .map(|&x| x as usize)
                                    .sum();
                                if sum_found_md5 == block_info.sum_md5 {
                                    // success
                                    if current_skip < skip {
                                        current_skip += 1;
                                    } else {
                                        result.push((found_key, found_data));
                                        current += 1;
                                    }
                                }
                            }
                        }
                    }
                    block_info = EMPTY_BLOCK;
                }
                END => {
                    break;
                }
                _ => {
                    tmp_group.push(v);
                }
            }
        }
    }
    result
}

fn get_find_next_data(
    read: &mut File,
    list_block_data: Vec<u8>,
    key: Vec<u8>,
    limit: u8,
    only_after_key: bool,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut result: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    {
        let mut block_info = EMPTY_BLOCK;
        let mut tmp_group: Vec<u8> = Vec::new();
        let mut current: u8 = 0;
        let len_current_key = key.len();
        let sum_current_key: usize = key.iter().map(|&x| x as usize).sum();
        let sum_current_md5: usize = md5::compute(&key)
            .to_vec()
            .iter()
            .map(|&x| x as usize)
            .sum();
        let mut check_is_begin = false;
        for v in list_block_data {
            if current >= limit {
                break;
            }
            match v {
                START => {
                    block_info.start = digits_to_number(&tmp_group);
                    tmp_group.clear();
                }
                SIZE_KEY => {
                    block_info.size_key = digits_to_number(&tmp_group);
                    tmp_group.clear();
                }
                SUM_KEY => {
                    block_info.sum_key = digits_to_number(&tmp_group);
                    tmp_group.clear();
                }
                SUM_MD5 => {
                    block_info.sum_md5 = digits_to_number(&tmp_group);
                    tmp_group.clear();
                }
                SIZE_DATA => {
                    block_info.size_data = digits_to_number(&tmp_group);
                    tmp_group.clear();
                    {
                        if !check_is_begin
                            && block_info.size_key == len_current_key
                            && block_info.sum_key == sum_current_key
                            && block_info.sum_md5 == sum_current_md5
                        {
                            let found_key =
                                pull_key(read, &block_info).unwrap_or_else(|_| Vec::new());
                            check_is_begin = found_key == key;
                        }
                        if check_is_begin {
                            // success
                            let (found_key, found_data) = pull_data(read, &block_info)
                                .unwrap_or_else(|_| (Vec::new(), Vec::new()));
                            let len_found_key = found_key.len();
                            if len_found_key == block_info.size_key {
                                let sum_found_key: usize =
                                    found_key.iter().map(|&x| x as usize).sum();
                                if sum_found_key == block_info.sum_key {
                                    let sum_found_md5: usize = md5::compute(&found_key)
                                        .to_vec()
                                        .iter()
                                        .map(|&x| x as usize)
                                        .sum();
                                    if sum_found_md5 == block_info.sum_md5 {
                                        // success
                                        if !only_after_key || current > 0 {
                                            result.push((found_key, found_data));
                                        }
                                        current += 1;
                                    }
                                }
                            }
                        }
                    }
                    block_info = EMPTY_BLOCK;
                }
                END => {
                    break;
                }
                _ => {
                    tmp_group.push(v);
                }
            }
        }
    }
    result
}

fn delete_to_data(
    read: &mut File,
    write: &mut File,
    start_list_point: usize,
    list_block_data: Vec<u8>,
    also_delete_the_found_block: bool,
    key: Vec<u8>,
) -> Result<()> {
    let mut this_found_index: usize = 0;
    let mut this_found_finish_index: usize = 0;
    let mut last_found_block_info: Block = EMPTY_BLOCK;
    {
        let mut block_control_index_before: usize = 0;
        let mut block_info = EMPTY_BLOCK;
        let mut tmp_group: Vec<u8> = Vec::new();
        let len_current_key = key.len();
        let sum_current_key: usize = key.iter().map(|&x| x as usize).sum();
        let sum_current_md5: usize = md5::compute(&key)
            .to_vec()
            .iter()
            .map(|&x| x as usize)
            .sum();
        for i in 0..list_block_data.len() {
            let v = list_block_data[i];
            match v {
                START => {
                    block_info.start = digits_to_number(&tmp_group);
                    tmp_group.clear();
                }
                SIZE_KEY => {
                    block_info.size_key = digits_to_number(&tmp_group);
                    tmp_group.clear();
                }
                SUM_KEY => {
                    block_info.sum_key = digits_to_number(&tmp_group);
                    tmp_group.clear();
                }
                SUM_MD5 => {
                    block_info.sum_md5 = digits_to_number(&tmp_group);
                    tmp_group.clear();
                }
                SIZE_DATA => {
                    block_info.size_data = digits_to_number(&tmp_group);
                    tmp_group.clear();
                    {
                        if block_info.size_key == len_current_key
                            && block_info.sum_key == sum_current_key
                            && block_info.sum_md5 == sum_current_md5
                        {
                            let found_key =
                                pull_key(read, &block_info).unwrap_or_else(|_| Vec::new());
                            if found_key == key {
                                // success
                                last_found_block_info = block_info;
                                this_found_index = block_control_index_before + 1;
                                this_found_finish_index = i + 1;
                            }
                        }
                    }
                    block_info = EMPTY_BLOCK;
                    block_control_index_before = i;
                }
                END => {
                    break;
                }
                _ => {
                    tmp_group.push(v);
                }
            }
        }
    }
    if this_found_index == 0
        || last_found_block_info.start == 0
        || last_found_block_info.size_key == 0
        || last_found_block_info.sum_key == 0
        || last_found_block_info.sum_md5 == 0
    {
        return Ok(());
    }
    if also_delete_the_found_block {
        update_list_block(
            write,
            start_list_point,
            list_block_data[this_found_finish_index..].to_vec(),
        )
    } else {
        update_list_block(
            write,
            start_list_point,
            list_block_data[this_found_index..].to_vec(),
        )
    }
}

fn delete_one_data(
    read: &mut File,
    write: &mut File,
    list_block_data: Vec<u8>,
    key: Vec<u8>,
    start_list_point: usize,
) -> Result<()> {
    let (new_list_block_data, _) = get_new_list_not_contain_key(read, list_block_data, key, false);
    update_list_block(write, start_list_point, new_list_block_data)
}

fn set_one_data(
    read: &mut File,
    write: &mut File,
    list_block_data: Vec<u8>,
    key: Vec<u8>,
    data: Vec<u8>,
    start_list_point: usize,
) -> Result<()> {
    let (new_list_block_data, new_list_block_info) =
        get_new_list_not_contain_key(read, list_block_data, key.clone(), true);

    let size_key = key.len();
    let size_data = data.len();
    let block_size = size_key + size_data;
    let sum_key: usize = key.iter().map(|&x| x as usize).sum();
    let sum_md5: usize = md5::compute(&key)
        .to_vec()
        .iter()
        .map(|&x| x as usize)
        .sum();

    let list_space = get_list_space(start_list_point, new_list_block_info);
    let (start_list, start_block) = get_perfect_space(list_space, start_list_point, block_size);
    let info_data = push_block_to_data(
        Vec::new(),
        &Block {
            start: start_block,
            size_key,
            sum_key,
            sum_md5,
            size_data,
        },
    );

    let list_block_data = merge_vec(&[new_list_block_data, info_data]);
    update_list_block(write, start_list, list_block_data)?;

    write.seek(Start(start_block as u64))?;
    write.write_all(&merge_vec(&[key, data]))
}

fn set_many_data(
    read: &mut File,
    write: &mut File,
    list_block_data: Vec<u8>,
    list_data: Vec<(Vec<u8>, Vec<u8>)>,
    start_list_point: usize,
) -> Result<()> {
    let (new_list_block_data, new_list_block_info) =
        get_new_list_not_contain_list_key(read, list_block_data, &list_data, true);

    let mut list_block_insert_position: Vec<String> = Vec::new();
    let mut min_size_block: usize = 0;
    let mut list_config_insert: Vec<Block> = Vec::new();
    {
        let mut max_size_block: usize = 0;
        for i in 0..list_data.len() {
            let (key, data) = list_data[i].clone();
            let size_key = key.len();
            let size_data = data.len();
            let block_size = size_key + size_data;
            if min_size_block == 0 || block_size < min_size_block {
                min_size_block = block_size;
            }
            if block_size > max_size_block {
                max_size_block = block_size;
            }
            let sum_key: usize = key.iter().map(|&x| x as usize).sum();
            let sum_md5: usize = md5::compute(&key)
                .to_vec()
                .iter()
                .map(|&x| x as usize)
                .sum();
            list_config_insert.push(Block {
                start: i,
                size_key,
                sum_key,
                sum_md5,
                size_data,
            });
            list_block_insert_position
                .push(format!("{}{}{}{}", size_key, sum_key, sum_md5, size_data));
        }

        list_config_insert.sort_by(|a, b| {
            let ka = a.size_key + a.size_data;
            let kb = b.size_key + b.size_data;
            kb.cmp(&ka)
        }); // sort decrement [ max ---> min ]
    }

    let mut start_list_block = start_list_point;
    let mut list_info_data: Vec<u8> = Vec::new();
    let mut selected: HashMap<usize, bool> = HashMap::new();
    let mut list_write_data: Vec<(usize, Vec<u8>, Vec<u8>)> = Vec::new();
    let mut total_last_space_used: usize = 0;
    let mut map_block_insert: HashMap<String, Block> = HashMap::new();

    {
        let list_space = get_list_space(start_list_point, new_list_block_info);
        for s in list_space {
            let mut this_space_used = 0;
            if s.size_data < min_size_block && s.sum_key == 0 {
                continue;
            }
            for c in list_config_insert.clone() {
                if selected.get(&c.start) == Some(&true) {
                    continue;
                }
                let block_size = c.size_key + c.size_data;
                if s.size_data >= block_size + this_space_used || s.sum_key == 1 {
                    selected.insert(c.start, true);
                    {
                        let start_block = s.start + this_space_used;
                        let (key, data) = list_data[c.start].clone();
                        add_to_map_sort(&mut map_block_insert, c, start_block);
                        list_write_data.push((start_block, key, data));
                        this_space_used += block_size;
                    }
                    if s.sum_key == 1 {
                        start_list_block = s.start;
                        total_last_space_used += block_size;
                    }
                }
            }
        }
    }

    // nếu những block còn lại không đủ khoảng trống thì xử lý thêm vào cuối
    for c in list_config_insert.clone() {
        if selected.get(&c.start) == Some(&true) {
            continue;
        }
        let block_size = c.size_key + c.size_data;
        let start_block = start_list_block + total_last_space_used;
        selected.insert(c.start, true);
        let (key, data) = list_data[c.start].clone();
        add_to_map_sort(&mut map_block_insert, c, start_block);
        list_write_data.push((start_block, key, data));
        total_last_space_used += block_size;
    }

    for key_sort in list_block_insert_position {
        match map_block_insert.get(&key_sort) {
            None => {
                continue;
            }
            Some(block) => {
                let info_data = push_block_to_data(Vec::new(), &block);
                list_info_data = merge_vec(&[list_info_data, info_data]);
            }
        }
    }

    let new_list_block_data = merge_vec(&[new_list_block_data, list_info_data]);
    update_list_block(
        write,
        start_list_block + total_last_space_used,
        new_list_block_data,
    )?;

    for (start_block, key, data) in list_write_data {
        write.seek(Start(start_block as u64))?;
        write.write_all(&merge_vec(&[key, data]))?;
    }
    Ok(())
}

fn add_to_map_sort(map_block_sort: &mut HashMap<String, Block>, c: Block, start_block: usize) {
    map_block_sort.insert(
        format!("{}{}{}{}", c.size_key, c.sum_key, c.sum_md5, c.size_data),
        Block {
            start: start_block,
            size_key: c.size_key,
            sum_key: c.sum_key,
            sum_md5: c.sum_md5,
            size_data: c.size_data,
        },
    );
}

fn get_new_list_not_contain_key(
    read: &mut File,
    list_block_data: Vec<u8>,
    key: Vec<u8>,
    is_return_list_info: bool,
) -> (Vec<u8>, Vec<Block>) {
    let mut new_list_block_data: Vec<u8> = Vec::new();
    let mut new_list_block_info: Vec<Block> = Vec::new();

    let len_key = key.len();
    let sum_key: usize = key.iter().map(|&x| x as usize).sum();
    let sum_md5: usize = md5::compute(&key)
        .to_vec()
        .iter()
        .map(|&x| x as usize)
        .sum();

    let mut block_info = EMPTY_BLOCK;
    let mut tmp_group: Vec<u8> = Vec::new();
    for v in list_block_data {
        match v {
            START => {
                block_info.start = digits_to_number(&tmp_group);
                tmp_group.clear();
            }
            SIZE_KEY => {
                block_info.size_key = digits_to_number(&tmp_group);
                tmp_group.clear();
            }
            SUM_KEY => {
                block_info.sum_key = digits_to_number(&tmp_group);
                tmp_group.clear();
            }
            SUM_MD5 => {
                block_info.sum_md5 = digits_to_number(&tmp_group);
                tmp_group.clear();
            }
            SIZE_DATA => {
                block_info.size_data = digits_to_number(&tmp_group);
                tmp_group.clear();
                {
                    {
                        if block_info.size_key == len_key
                            && block_info.sum_key == sum_key
                            && block_info.sum_md5 == sum_md5
                        {
                            let found_key =
                                pull_key(read, &block_info).unwrap_or_else(|_| Vec::new());
                            if found_key == key {
                                // success
                                continue;
                            }
                        }
                    }
                    new_list_block_data = push_block_to_data(new_list_block_data, &block_info);
                    if is_return_list_info {
                        new_list_block_info.push(block_info);
                    }
                }
                block_info = EMPTY_BLOCK;
            }
            END => {
                break;
            }
            _ => {
                tmp_group.push(v);
            }
        }
    }
    (new_list_block_data, new_list_block_info)
}

fn get_new_list_not_contain_list_key(
    read: &mut File,
    list_block_data: Vec<u8>,
    list_data: &Vec<(Vec<u8>, Vec<u8>)>,
    is_return_list_info: bool,
) -> (Vec<u8>, Vec<Block>) {
    let mut list_skip_check: HashMap<Vec<u8>, bool> = HashMap::new();

    let mut new_list_block_data: Vec<u8> = Vec::new();
    let mut new_list_block_info: Vec<Block> = Vec::new();

    let mut block_info = EMPTY_BLOCK;
    let mut tmp_group: Vec<u8> = Vec::new();
    for v in list_block_data {
        match v {
            START => {
                block_info.start = digits_to_number(&tmp_group);
                tmp_group.clear();
            }
            SIZE_KEY => {
                block_info.size_key = digits_to_number(&tmp_group);
                tmp_group.clear();
            }
            SUM_KEY => {
                block_info.sum_key = digits_to_number(&tmp_group);
                tmp_group.clear();
            }
            SUM_MD5 => {
                block_info.sum_md5 = digits_to_number(&tmp_group);
                tmp_group.clear();
            }
            SIZE_DATA => {
                block_info.size_data = digits_to_number(&tmp_group);
                tmp_group.clear();
                {
                    {
                        let mut is_found_key = false;
                        for (cur_key, _) in list_data {
                            if list_skip_check.get(cur_key) == Some(&true) {
                                continue;
                            }
                            let key = cur_key.clone();
                            let len_key = key.len();
                            let sum_key: usize = key.iter().map(|&x| x as usize).sum();
                            let sum_md5: usize = md5::compute(&key)
                                .to_vec()
                                .iter()
                                .map(|&x| x as usize)
                                .sum();
                            {
                                if block_info.size_key == len_key
                                    && block_info.sum_key == sum_key
                                    && block_info.sum_md5 == sum_md5
                                {
                                    let found_key =
                                        pull_key(read, &block_info).unwrap_or_else(|_| Vec::new());
                                    if found_key == key {
                                        // success
                                        is_found_key = true;
                                        list_skip_check.insert(key, true);
                                        break;
                                    }
                                }
                            }
                        }
                        if is_found_key {
                            continue;
                        }
                    }
                    new_list_block_data = push_block_to_data(new_list_block_data, &block_info);
                    if is_return_list_info {
                        new_list_block_info.push(block_info);
                    }
                }
                block_info = EMPTY_BLOCK;
            }
            END => {
                break;
            }
            _ => {
                tmp_group.push(v);
            }
        }
    }
    (new_list_block_data, new_list_block_info)
}

fn push_block_to_data(list_block_data: Vec<u8>, block_info: &Block) -> Vec<u8> {
    merge_vec(&[
        list_block_data,
        group_digits_to_vec(block_info.start),
        vec![START],
        group_digits_to_vec(block_info.size_key),
        vec![SIZE_KEY],
        group_digits_to_vec(block_info.sum_key),
        vec![SUM_KEY],
        group_digits_to_vec(block_info.sum_md5),
        vec![SUM_MD5],
        group_digits_to_vec(block_info.size_data),
        vec![SIZE_DATA],
    ])
}

fn get_list_space(start_list_point: usize, list_block_info: Vec<Block>) -> Vec<Block> {
    let mut list_start_block: Vec<usize> = Vec::new();
    let mut map_start_block: HashMap<usize, usize> = HashMap::new();
    for b in list_block_info {
        list_start_block.push(b.start);
        map_start_block.insert(b.start, b.size_key + b.size_data);
    }
    list_start_block.sort();

    let mut current_point = FIRST_SIZE;
    let mut list_space: Vec<Block> = Vec::new();
    for start in list_start_block {
        if current_point < start {
            list_space.push(Block {
                start: current_point,
                size_key: 0,
                sum_key: 0,
                sum_md5: 0,
                size_data: start - current_point,
            });
        }
        current_point = start + map_start_block[&start];
    }
    let last_space_size = start_list_point.saturating_sub(current_point);
    if last_space_size > 0 {
        list_space.push(Block {
            start: current_point,
            size_key: 0,
            sum_key: 1, // space này là vị trí còn trống cuối cùng trước list
            sum_md5: 0,
            size_data: last_space_size,
        });
    }
    list_space
}

fn get_perfect_space(
    list_space: Vec<Block>,
    start_list_point: usize,
    block_size: usize,
) -> (usize, usize) {
    let mut perfect_block_size: usize = block_size;
    let mut perfect_free_size: usize = 0;
    let mut start_block = start_list_point;
    let mut is_last_space: bool = false;
    for s in list_space {
        if s.size_data >= block_size || s.sum_key == 1 {
            let new_size = s.size_data;
            if perfect_free_size == 0
                || (perfect_free_size > new_size && start_list_point > s.start + s.size_data)
            {
                perfect_free_size = new_size;
                start_block = s.start;
                is_last_space = s.sum_key == 1;
            }
        }
    }
    if perfect_free_size >= perfect_block_size {
        perfect_block_size = 0
    }
    let start_list: usize;
    if is_last_space {
        start_list = start_block + block_size;
    } else {
        start_list = start_list_point + perfect_block_size;
    }
    (start_list, start_block)
}

fn get_list_config(read: &mut File) -> Result<(usize, Vec<u8>)> {
    read.seek(Start(0))?;
    let mut buffer = vec![0u8; FIRST_SIZE];
    read.read_exact(&mut buffer)?;

    let mut start_list_data = Vec::new();
    let mut size_list_data = Vec::new();
    {
        let mut position_list_check: u8 = 0;
        for v in buffer {
            if v == END {
                position_list_check += 1;
                continue;
            }
            if position_list_check == 0 {
                start_list_data.push(v)
            } else if position_list_check == 1 {
                size_list_data.push(v)
            } else {
                break;
            }
        }
    }
    // println!("0 === {:?} {:?}", start_list_data, size_list_data);

    let mut start_list_point = digits_to_number(&start_list_data);
    let mut list_block_data: Vec<u8>;
    {
        if start_list_point < FIRST_SIZE {
            start_list_point = FIRST_SIZE;
            list_block_data = Vec::new();
        } else {
            read.seek(Start(start_list_point as u64))?;
            let size_list = digits_to_number(&size_list_data);
            list_block_data = vec![0u8; size_list];
            read.read_exact(&mut list_block_data)?;
            if let Some(pos) = list_block_data.iter().position(|&x| x == END) {
                list_block_data.truncate(pos);
            }
        }
    }
    // println!("1 === {:?} {:?}", start_list_point, list_block_data);

    Ok((start_list_point, list_block_data))
}

fn update_list_block(write: &mut File, start: usize, list_block_data: Vec<u8>) -> Result<()> {
    let first_block_data = merge_vec(&[
        group_digits_to_vec(start),
        vec![END],
        group_digits_to_vec(list_block_data.len()),
        vec![END],
    ]);
    write.seek(Start(start as u64))?;
    write.write_all(&merge_vec(&[list_block_data, vec![END]]))?;
    write.seek(Start(0))?;
    write.write_all(&first_block_data)
}

#[cfg(test)]
mod test_md5 {
    #[test]
    fn test_md5() {
        let digest = md5::compute(b"abcdefghijklmnopqrstuvwxyz");
        assert_eq!(format!("{:x}", digest), "c3fcd3d76192e4007dfb496cca67e13b");
    }
}

impl Trait for Bucket {
    fn new(path: String) -> Result<Self> {
        let reader = match File::open(&path) {
            Ok(f) => f,
            Err(_) => {
                File::create(&path)?;
                File::open(&path)?
            }
        };
        let writer = OpenOptions::new().write(true).open(&path)?;

        Ok(Self { reader, writer })
    }

    fn set(&mut self, key: Vec<u8>, data: Vec<u8>) -> Result<()> {
        self.writer.lock()?;
        let (start_list_point, list_block_data) =
            get_list_config(&mut self.reader).unwrap_or_else(|_| (FIRST_SIZE, Vec::new()));
        set_one_data(
            &mut self.reader,
            &mut self.writer,
            list_block_data,
            key,
            data,
            start_list_point,
        )?;
        self.writer.unlock()
    }

    fn get(&mut self, key: Vec<u8>) -> (Vec<u8>, Vec<u8>) {
        let (_, list_block_data) =
            get_list_config(&mut self.reader).unwrap_or_else(|_| (FIRST_SIZE, Vec::new()));
        get_one_data(&mut self.reader, list_block_data, key)
    }

    fn delete(&mut self, key: Vec<u8>) -> Result<()> {
        self.writer.lock()?;
        let (start_list_point, list_block_data) =
            get_list_config(&mut self.reader).unwrap_or_else(|_| (FIRST_SIZE, Vec::new()));
        delete_one_data(
            &mut self.reader,
            &mut self.writer,
            list_block_data,
            key,
            start_list_point,
        )?;
        self.writer.unlock()
    }

    fn set_many(&mut self, list_data: Vec<(Vec<u8>, Vec<u8>)>) -> Result<()> {
        self.writer.lock()?;
        let (start_list_point, list_block_data) =
            get_list_config(&mut self.reader).unwrap_or_else(|_| (FIRST_SIZE, Vec::new()));
        set_many_data(
            &mut self.reader,
            &mut self.writer,
            list_block_data,
            list_data,
            start_list_point,
        )?;
        self.writer.unlock()
    }

    fn list(&mut self, limit: u8) -> Vec<(Vec<u8>, Vec<u8>)> {
        let (_, list_block_data) =
            get_list_config(&mut self.reader).unwrap_or_else(|_| (FIRST_SIZE, Vec::new()));
        get_list_data(&mut self.reader, list_block_data, limit)
    }

    fn list_next(&mut self, limit: u8, skip: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        let (_, list_block_data) =
            get_list_config(&mut self.reader).unwrap_or_else(|_| (FIRST_SIZE, Vec::new()));
        get_list_next_data(&mut self.reader, list_block_data, limit, skip)
    }

    fn find_next(
        &mut self,
        key: Vec<u8>,
        limit: u8,
        only_after_key: bool,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let (_, list_block_data) =
            get_list_config(&mut self.reader).unwrap_or_else(|_| (FIRST_SIZE, Vec::new()));
        get_find_next_data(
            &mut self.reader,
            list_block_data,
            key,
            limit,
            only_after_key,
        )
    }

    fn delete_to(&mut self, key: Vec<u8>, also_delete_the_found_block: bool) -> Result<()> {
        self.writer.lock()?;
        let (start_list_point, list_block_data) =
            get_list_config(&mut self.reader).unwrap_or_else(|_| (FIRST_SIZE, Vec::new()));
        delete_to_data(
            &mut self.reader,
            &mut self.writer,
            start_list_point,
            list_block_data,
            also_delete_the_found_block,
            key,
        )?;
        self.writer.unlock()
    }

    fn list_lock_delete(&mut self, limit: u8) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.reader.lock()?;
        let (start_list_point, list_block_data) =
            get_list_config(&mut self.reader).unwrap_or_else(|_| (FIRST_SIZE, Vec::new()));
        let result = get_list_lock_delete_data(
            &mut self.reader,
            &mut self.writer,
            start_list_point,
            list_block_data,
            limit,
        );
        self.reader.unlock()?;
        result
    }
}

#[cfg(test)]
mod tests {
    use crate::{Bucket, Trait};
    use std::fs;

    #[test]
    fn test_all() {
        set_data();
        get_data();
        delete_data();
        set_many_data();
        list_data();
        list_next_data();
        find_next_data();
        delete_to_data();
        get_list_and_delete_list_data();
        delete_bucket();
    }

    fn set_data() {
        let file_path = String::from("data.db");
        let mut bucket = Bucket::new(file_path).unwrap();

        let test_key: Vec<u8> = String::from("test-key-001-99999999999999").into_bytes();
        let test_value: Vec<u8> = String::from("test data value: 0123456789 abcdefgh").into_bytes();
        let error = bucket.set(test_key, test_value).is_err();

        assert_eq!(error, false);
    }

    fn get_data() {
        let file_path = String::from("data.db");
        let mut bucket = Bucket::new(file_path).unwrap();

        let test_key: Vec<u8> = String::from("test-key-001-99999999999999").into_bytes();
        let test_value: Vec<u8> = String::from("test data value: 0123456789 abcdefgh").into_bytes();
        let (key_block, value_block) = bucket.get(test_key.clone());

        assert_eq!(test_key, key_block);
        assert_eq!(test_value, value_block);
    }

    fn delete_data() {
        let file_path = String::from("data.db");
        let mut bucket = Bucket::new(file_path).unwrap();

        let test_key: Vec<u8> = String::from("test-key-001-99999999999999").into_bytes();
        let error = bucket.delete(test_key).is_err();

        assert_eq!(error, false);
    }

    fn set_many_data() {
        let file_path = String::from("data.db");
        let mut bucket = Bucket::new(file_path).unwrap();

        // let test_key: Vec<u8> = String::from("test-key-001-99999999999999").into_bytes();
        let test_value: Vec<u8> = String::from("test data value: 0123456789 abcdefgh").into_bytes();

        let mut list_data: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for i in 0..10 {
            let test_key = format!("test-key-00{}-99999999999999", i).into_bytes();
            list_data.push((test_key, test_value.clone()));
        }

        let error = bucket.set_many(list_data).is_err();

        assert_eq!(error, false);
    }

    fn list_data() {
        let file_path = String::from("data.db");
        let mut bucket = Bucket::new(file_path).unwrap();

        let limit = 10u8;
        let list_block = bucket.list(limit);

        assert_eq!(list_block.len() > 0, true);
    }

    fn list_next_data() {
        let file_path = String::from("data.db");
        let mut bucket = Bucket::new(file_path).unwrap();

        let limit = 10u8;
        let skip = 0usize;
        let list_block = bucket.list_next(limit, skip);

        assert_eq!(list_block.len() > 0, true);
    }

    fn find_next_data() {
        let file_path = String::from("data.db");
        let mut bucket = Bucket::new(file_path).unwrap();

        let test_key: Vec<u8> = String::from("test-key-001-99999999999999").into_bytes();
        let limit = 10u8;
        let only_after_key = false;
        // let only_after_key = true;
        let list_block = bucket.find_next(test_key, limit, only_after_key);

        assert_eq!(list_block.len() > 0, true);
    }

    fn delete_to_data() {
        let file_path = String::from("data.db");
        let mut bucket = Bucket::new(file_path).unwrap();

        let test_key: Vec<u8> = String::from("test-key-001-99999999999999").into_bytes();
        let also_delete_the_found_block = true;
        // let also_delete_the_found_block = false;
        let error = bucket
            .delete_to(test_key, also_delete_the_found_block)
            .is_err();

        assert_eq!(error, false);
    }

    fn get_list_and_delete_list_data() {
        let file_path = String::from("data.db");
        let mut bucket = Bucket::new(file_path).unwrap();

        let limit = 10u8;
        let list_block = bucket.list_lock_delete(limit).unwrap();

        assert_eq!(list_block.len() > 0, true);
    }

    fn delete_bucket() {
        let file_path = String::from("data.db");
        fs::remove_file(file_path).unwrap()
    }
}

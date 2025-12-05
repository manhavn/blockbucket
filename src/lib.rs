use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom::Start, Write};

pub trait Trait {
    fn new(file_path: String) -> Self;
    fn get(&mut self, key: Vec<u8>) -> (Vec<u8>, Vec<u8>);
    fn delete(&mut self, key: Vec<u8>) -> std::io::Result<()>;
    fn delete_to(&mut self, key: Vec<u8>, only_before_key: bool) -> std::io::Result<()>;
    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> std::io::Result<()>;
    fn list(&mut self, count: u8) -> Vec<(Vec<u8>, Vec<u8>)>;
    fn find_next(
        &mut self,
        key: Vec<u8>,
        count: u8,
        only_after_key: bool,
    ) -> Vec<(Vec<u8>, Vec<u8>)>;
}

pub struct Bucket {
    pub(crate) read: File,
    pub(crate) write: File,
}

const MAX_DIGIT_GROUP: u16 = 250;
const START: u8 = 251;
const SIZE_KEY: u8 = 252;
const SUM_KEY: u8 = 253;
const SIZE_DATA: u8 = 254;
const END: u8 = 255;
const FIRST_SIZE: usize = 128;

#[derive(Clone)]
struct Block {
    pub start: usize,
    pub size_key: usize,
    pub sum_key: usize,
    pub size_data: usize,
}

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
        // thử lấy 3 chữ số
        if i + 2 < digits.len() {
            let v = (digits[i] as u16) * 100 + (digits[i + 1] as u16) * 10 + (digits[i + 2] as u16);
            if v <= MAX_DIGIT_GROUP {
                result.push(v as u8);
                i += 3;
                continue;
            }
        }

        // thử lấy 2 chữ số
        if i + 1 < digits.len() {
            result.push((digits[i]) * 10 + (digits[i + 1]));
            i += 2;
            continue;
        }

        // fallback: 1 chữ số
        result.push(digits[i]);
        i += 1;
    }
    result
}

fn digits_to_number(digits: &[u8]) -> usize {
    let mut n: usize = 0;
    for &d in digits {
        n = n * 10 + d as usize;
    }
    n
}

fn merge_vec(vec: &[Vec<u8>]) -> Vec<u8> {
    let total: usize = vec.iter().map(|v| v.len()).sum();
    let mut out = Vec::with_capacity(total);
    for v in vec {
        out.extend_from_slice(v);
    }
    out
}

fn convert_data_to_info(list_block_data: Vec<u8>) -> Vec<Block> {
    let mut list_block_info: Vec<Block> = Vec::new();
    {
        let mut block_info = Block {
            start: 0,
            size_key: 0,
            sum_key: 0,
            size_data: 0,
        };
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
                SIZE_DATA => {
                    if block_info.size_key > 0 {
                        block_info.size_data = digits_to_number(&tmp_group);
                        list_block_info.push(block_info.clone());
                    }
                    tmp_group.clear();
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
    list_block_info
}

fn convert_data_to_info_limit(list_block_data: Vec<u8>, count: u8) -> Vec<Block> {
    let mut list_block_info: Vec<Block> = Vec::new();
    {
        let mut block_info = Block {
            start: 0,
            size_key: 0,
            sum_key: 0,
            size_data: 0,
        };
        let mut tmp_group: Vec<u8> = Vec::new();
        let mut current: u8 = 0;
        for v in list_block_data {
            if current >= count {
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
                SIZE_DATA => {
                    if block_info.size_key > 0 {
                        block_info.size_data = digits_to_number(&tmp_group);
                        list_block_info.push(block_info.clone());
                        current += 1;
                    }
                    tmp_group.clear();
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
    list_block_info
}

fn convert_data_to_info_find_next(
    file: &mut File,
    list_block_data: Vec<u8>,
    key: Vec<u8>,
    count: u8,
    only_after_key: bool,
) -> Vec<Block> {
    let mut list_block_info: Vec<Block> = Vec::new();
    {
        let mut block_info = Block {
            start: 0,
            size_key: 0,
            sum_key: 0,
            size_data: 0,
        };
        let mut tmp_group: Vec<u8> = Vec::new();
        let mut current: u8 = 0;
        let len_current_key = key.len();
        let sum_current_key: u64 = key.iter().map(|&x| x as u64).sum();
        let mut check_is_begin = false;
        for v in list_block_data {
            if current >= count {
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
                SIZE_DATA => {
                    if block_info.size_key > 0 {
                        block_info.size_data = digits_to_number(&tmp_group);
                        if !check_is_begin
                            && block_info.size_key == len_current_key
                            && block_info.sum_key == sum_current_key as usize
                        {
                            if !file.seek(Start(block_info.start as u64)).is_err() {
                                let mut found_key = vec![0u8; block_info.size_key];
                                if !file.read_exact(&mut found_key).is_err() {
                                    check_is_begin = found_key == key.clone();
                                };
                            }
                        }
                        if check_is_begin {
                            if !only_after_key || current > 0 {
                                list_block_info.push(block_info.clone());
                            }
                            current += 1;
                        }
                    }
                    tmp_group.clear();
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
    list_block_info
}

fn push_block_to_data(list_block_data: Vec<u8>, block_info: Block) -> Vec<u8> {
    merge_vec(&[
        list_block_data,
        group_digits_to_vec(block_info.start),
        vec![START],
        group_digits_to_vec(block_info.size_key),
        vec![SIZE_KEY],
        group_digits_to_vec(block_info.sum_key),
        vec![SUM_KEY],
        group_digits_to_vec(block_info.size_data),
        vec![SIZE_DATA],
    ])
}

fn get_list_space(end_data_size: usize, list_block_info: Vec<Block>) -> Vec<Block> {
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
                size_data: start - current_point,
            });
        }
        current_point = start + map_start_block[&start];
    }
    let last_space_size = end_data_size.saturating_sub(current_point);
    if last_space_size > 0 {
        list_space.push(Block {
            start: current_point,
            size_key: 0,
            sum_key: 1, // space này là vị trí còn trống cuối cùng trước list
            size_data: last_space_size,
        });
    }
    list_space
}

fn get_perfect_space(
    list_space: Vec<Block>,
    end_data_size: usize,
    key: &Vec<u8>,
    value: &Vec<u8>,
) -> (usize, usize, bool) {
    let data_size = key.len() + value.len();
    let mut perfect_block_size: usize = data_size;
    let mut perfect_size: usize = 0;
    let mut perfect_start_block = end_data_size;
    let mut is_last_space: bool = false;
    for s in list_space {
        if s.size_data >= data_size || s.sum_key == 1 {
            let new_size = s.size_data;
            if perfect_size == 0
                || (perfect_size > new_size && end_data_size > s.start + s.size_data)
            {
                perfect_size = new_size;
                perfect_start_block = s.start;
                is_last_space = s.sum_key == 1;
            }
        }
    }
    if perfect_size >= perfect_block_size {
        perfect_block_size = 0
    }
    (perfect_block_size, perfect_start_block, is_last_space)
}

fn get_end_data_size(file: &mut File) -> (usize, usize, Vec<u8>) {
    let zero_result = (FIRST_SIZE, 0, Vec::new());
    if file.seek(Start(0)).is_err() {
        return zero_result;
    }

    let mut buffer = vec![0u8; FIRST_SIZE];
    if file.read_exact(&mut buffer).is_err() {
        return zero_result;
    }

    let mut position_list_check: u8 = 0;
    let mut begin_list_position = Vec::new();
    let mut end_list_position = Vec::new();
    for v in buffer {
        if v == END {
            position_list_check += 1;
            continue;
        }
        if position_list_check == 0 {
            begin_list_position.push(v)
        } else if position_list_check == 1 {
            end_list_position.push(v)
        } else {
            break;
        }
    }

    let mut end_data_size = digits_to_number(&begin_list_position);
    let end_list_size = digits_to_number(&end_list_position);
    let mut list_block_data: Vec<u8> = vec![0u8; end_list_size];
    if end_data_size == 0 {
        end_data_size = FIRST_SIZE;
    } else {
        if file.seek(Start(end_data_size as u64)).is_err() {
            return zero_result;
        }
        if file.read_exact(&mut list_block_data).is_err() {
            return zero_result;
        }

        if let Some(pos) = list_block_data.iter().position(|&x| x == END) {
            list_block_data.truncate(pos);
        }
    }

    (end_data_size, list_block_data.len(), list_block_data)
}

fn get_block_info(file: &mut File, list_block_info: &Vec<Block>, find_key: Vec<u8>) -> Vec<Block> {
    let sum_find_key: u64 = find_key.iter().map(|&x| x as u64).sum();
    let mut result = Vec::new();
    let len_find_key = find_key.len();
    if len_find_key > 0 && sum_find_key > 0 {
        for v in list_block_info {
            if len_find_key == v.size_key && sum_find_key as usize == v.sum_key {
                if file.seek(Start(v.start as u64)).is_err() {
                    continue;
                };
                let mut found_key = vec![0u8; len_find_key];
                if file.read_exact(&mut found_key).is_err() {
                    continue;
                };
                if *find_key == found_key {
                    result.push(v.clone());
                }
            }
        }
    }
    result
}

fn update_list_block(
    file: &mut File,
    start: usize,
    list_block_data: Vec<u8>,
) -> std::io::Result<Vec<u8>> {
    let first_block_data = merge_vec(&[
        group_digits_to_vec(start),
        vec![END],
        group_digits_to_vec(list_block_data.len()),
        vec![END],
    ]);
    file.seek(Start(start as u64))?;
    file.write_all(&merge_vec(&[list_block_data, vec![END]]))?;
    file.seek(Start(0))?;
    file.write_all(&first_block_data)?;
    Ok(first_block_data)
}

fn get_block(file: &mut File, list_found: Vec<Block>) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut result: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for f in list_found {
        let mut block = (Vec::new(), Vec::new());
        if file.seek(Start(f.start as u64)).is_err() {
            continue;
        };
        let mut found_key = vec![0u8; f.size_key];
        if file.read_exact(&mut found_key).is_err() {
            continue;
        };
        if file.seek(Start((f.start + f.size_key) as u64)).is_err() {
            continue;
        };
        let mut found_value = vec![0u8; f.size_data];
        if file.read_exact(&mut found_value).is_err() {
            continue;
        };
        block.0 = found_key;
        block.1 = found_value;
        result.push(block);
    }
    result
}

fn remove_block(
    file: &mut File,
    end_data_size: usize,
    list_block_info: Vec<Block>,
    list_delete: Vec<Block>,
) -> std::io::Result<()> {
    let mut list_block_data: Vec<u8> = Vec::new();
    let mut map_skip: HashMap<usize, bool> = HashMap::new();
    for l in list_block_info.clone() {
        for f in list_delete.clone() {
            if l.start == f.start {
                map_skip.insert(l.start, true);
                break;
            }
        }
        if map_skip.get(&l.start) == Some(&true) {
            continue;
        }
        list_block_data = push_block_to_data(list_block_data, l);
    }
    update_list_block(file, end_data_size, list_block_data)?;
    Ok(())
}

fn remove_block_to(
    read: &mut File,
    write: &mut File,
    end_data_size: usize,
    list_block_info: Vec<Block>,
    also_delete_the_found_block: bool,
    find_key: Vec<u8>,
) -> std::io::Result<()> {
    let mut list_block_data: Vec<u8> = Vec::new();
    let sum_find_key: u64 = find_key.iter().map(|&x| x as u64).sum();
    let len_find_key = find_key.len();
    let len_list_block_info = list_block_info.len();
    let mut last_index_found = None;
    for i in (0..len_list_block_info).rev() {
        let b = &list_block_info[i];
        if b.size_key == len_find_key && b.sum_key == sum_find_key as usize {
            if read.seek(Start(b.start as u64)).is_err() {
                continue;
            };
            let mut found_key = vec![0u8; len_find_key];
            if read.read_exact(&mut found_key).is_err() {
                continue;
            };
            if *find_key == found_key {
                last_index_found = Some(i);
            }
            break;
        }
    }
    let Some(last_index_found) = last_index_found else {
        return Ok(());
    };
    for i in 0..len_list_block_info {
        if i > last_index_found || (last_index_found == i && !also_delete_the_found_block) {
            let block = &list_block_info[i];
            list_block_data = push_block_to_data(list_block_data, block.clone());
        }
    }
    update_list_block(write, end_data_size, list_block_data)?;
    Ok(())
}

fn add_block(
    file: &mut File,
    start: usize,
    key: Vec<u8>,
    data: Vec<u8>,
) -> std::io::Result<(usize, Vec<u8>)> {
    let len_key = key.len();
    let sum_key: u64 = key.iter().map(|&x| x as u64).sum();
    let len_data = data.len();
    let block_size = len_key + len_data;
    let block_data = merge_vec(&[key, data]);

    file.seek(Start(start as u64))?;
    file.write_all(&block_data)?;

    let info_data = push_block_to_data(
        vec![],
        Block {
            start,
            size_key: len_key,
            sum_key: sum_key as usize,
            size_data: len_data,
        },
    );
    Ok((block_size, info_data))
}

impl Trait for Bucket {
    fn new(file_path: String) -> Self {
        let read_file = match File::open(&file_path) {
            Ok(f) => f,
            Err(_) => File::create(&file_path).unwrap(),
        };
        let write_file = OpenOptions::new().write(true).open(&file_path).unwrap();

        Self {
            read: read_file,
            write: write_file,
        }
    }

    fn get(&mut self, key: Vec<u8>) -> (Vec<u8>, Vec<u8>) {
        let (_, _, list_block_data) = get_end_data_size(&mut self.read);
        let list_block_info = convert_data_to_info(list_block_data);
        let list_found = get_block_info(&mut self.read, &list_block_info, key);
        let list_block = get_block(&mut self.read, list_found);
        let mut result: (Vec<u8>, Vec<u8>) = (Vec::new(), Vec::new());
        for b in list_block {
            result.0 = b.0;
            result.1 = b.1;
            break;
        }
        result
    }

    fn delete(&mut self, key: Vec<u8>) -> std::io::Result<()> {
        let (end_data_size, _, list_block_data) = get_end_data_size(&mut self.read);
        let list_block_info = convert_data_to_info(list_block_data);
        let list_found = get_block_info(&mut self.read, &list_block_info, key);
        remove_block(&mut self.write, end_data_size, list_block_info, list_found)
    }

    fn delete_to(
        &mut self,
        key: Vec<u8>,
        also_delete_the_found_block: bool,
    ) -> std::io::Result<()> {
        let (end_data_size, _, list_block_data) = get_end_data_size(&mut self.read);
        let list_block_info = convert_data_to_info(list_block_data);
        remove_block_to(
            &mut self.read,
            &mut self.write,
            end_data_size,
            list_block_info,
            also_delete_the_found_block,
            key,
        )
    }

    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> std::io::Result<()> {
        let (end_data_size, _, list_block_data) = get_end_data_size(&mut self.read);
        let list_block_info = convert_data_to_info(list_block_data.clone());

        let list_space = get_list_space(end_data_size, list_block_info);

        let (perfect_space_size, perfect_start_space, is_last_space) =
            get_perfect_space(list_space, end_data_size, &key, &value);

        let (block_size, info_data) = add_block(&mut self.write, perfect_start_space, key, value)?;

        let mut start_list_block = end_data_size + perfect_space_size;
        if is_last_space {
            start_list_block = perfect_start_space + block_size;
        }

        update_list_block(
            &mut self.write,
            start_list_block,
            merge_vec(&[list_block_data, info_data]),
        )?;
        Ok(())
    }

    fn list(&mut self, count: u8) -> Vec<(Vec<u8>, Vec<u8>)> {
        let (_, _, list_block_data) = get_end_data_size(&mut self.read);
        let list_block_info = convert_data_to_info_limit(list_block_data, count);
        get_block(&mut self.read, list_block_info)
    }

    fn find_next(
        &mut self,
        key: Vec<u8>,
        count: u8,
        only_after_key: bool,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let (_, _, list_block_data) = get_end_data_size(&mut self.read);
        let list_block_info = convert_data_to_info_find_next(
            &mut self.read,
            list_block_data,
            key,
            count,
            only_after_key,
        );
        get_block(&mut self.read, list_block_info)
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
        list_data();
        find_next_data();
        delete_data();
        delete_to_data();
        delete_bucket();
    }

    fn set_data() {
        let file_path = String::from("data.db");
        let mut bucket = Bucket::new(file_path);

        let test_key: Vec<u8> = String::from("test-key-001-99999999999999").into_bytes();
        let test_value: Vec<u8> = String::from("test data value: 0123456789 abcdefgh").into_bytes();
        let error = bucket.set(test_key, test_value).is_err();

        assert_eq!(error, false);
    }

    fn get_data() {
        let file_path = String::from("data.db");
        let mut bucket = Bucket::new(file_path);

        let test_key: Vec<u8> = String::from("test-key-001-99999999999999").into_bytes();
        let test_value: Vec<u8> = String::from("test data value: 0123456789 abcdefgh").into_bytes();
        let (key_block, value_block) = bucket.get(test_key.clone());

        assert_eq!(test_key, key_block);
        assert_eq!(test_value, value_block);
    }

    fn list_data() {
        let file_path = String::from("data.db");
        let mut bucket = Bucket::new(file_path);

        let count = 10u8;
        let list_block = bucket.list(count);

        assert_eq!(list_block.len() > 0, true);
    }

    fn find_next_data() {
        let file_path = String::from("data.db");
        let mut bucket = Bucket::new(file_path);

        let test_key: Vec<u8> = String::from("test-key-001-99999999999999").into_bytes();
        let count = 10u8;
        let only_after_key = false;
        // let only_after_key = true;
        let list_block = bucket.find_next(test_key, count, only_after_key);

        assert_eq!(list_block.len() > 0, true);
    }

    fn delete_data() {
        let file_path = String::from("data.db");
        let mut bucket = Bucket::new(file_path);

        let test_key: Vec<u8> = String::from("test-key-001-99999999999999").into_bytes();
        let error = bucket.delete(test_key).is_err();

        assert_eq!(error, false);
    }

    fn delete_to_data() {
        let file_path = String::from("data.db");
        let mut bucket = Bucket::new(file_path);

        let test_key: Vec<u8> = String::from("test-key-001-99999999999999").into_bytes();
        let also_delete_the_found_block = true;
        // let also_delete_the_found_block = false;
        let error = bucket
            .delete_to(test_key, also_delete_the_found_block)
            .is_err();

        assert_eq!(error, false);
    }

    fn delete_bucket() {
        let file_path = String::from("data.db");
        fs::remove_file(file_path).unwrap()
    }
}

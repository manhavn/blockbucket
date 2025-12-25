# SETUP

- Github: [https://github.com/manhavn/blockbucket](https://github.com/manhavn/blockbucket)
- Crate: [https://crates.io/crates/blockbucket](https://crates.io/crates/blockbucket)

```shell
 cargo add blockbucket
```

- `Cargo.toml`

```toml
# ...

[dependencies]
#blockbucket = { git = "https://github.com/manhavn/blockbucket.git" }
blockbucket = "0.2.7" # https://crates.io/crates/blockbucket
```

- `test.rs`

```rust
#[cfg(test)]
mod tests {
    use blockbucket::{Bucket, Trait};
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

    fn delete_bucket() {
        let file_path = String::from("data.db");
        fs::remove_file(file_path).unwrap()
    }
}
```

# DEMO queue app

```rust
use blockbucket::{Bucket, Trait};
use std::time::{Duration, Instant};
use std::{fs, thread};

fn main() {
    thread::spawn(|| {
        loop {
            add_queue();
            thread::sleep(Duration::from_secs(20));
        }
    });

    loop {
        run_queue();
        thread::sleep(Duration::from_secs(2));
    }
}

fn add_queue() {
    let file_path = String::from("data.db");
    let mut bucket = Bucket::new(file_path.clone()).unwrap();

    let time_md5 = md5::compute(format!("{:?}", Instant::now().to_owned()));
    let mut list_data: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

    for i in 0..10 {
        let test_key = format!("test-key-{}-{:x}", i, time_md5).into_bytes();
        let test_data = format!("test-data-{}-{:x}", i, time_md5).into_bytes();
        list_data.push((test_key, test_data.clone()));
    }

    let error = bucket.set_many(list_data).is_err();
    let bucket_size = match fs::metadata(file_path) {
        Ok(metadata) => metadata.len(),
        Err(_) => 0,
    };

    println!(
        "Queue added {}, Bucket file size {} bytes",
        error == false,
        bucket_size
    );
}

fn run_queue() {
    let file_path = String::from("data.db");
    let mut bucket = Bucket::new(file_path.clone()).unwrap();

    let limit = 3u8;
    // let list_block = bucket.list(limit);
    let list_block = bucket.list_lock_delete(limit).unwrap();

    // let mut end_key: Vec<u8> = Vec::new();
    for (k, v) in list_block {
        // end_key = k.clone();
        let key = String::from_utf8(k).unwrap();
        let value = String::from_utf8(v).unwrap();

        println!("{:?} ==> {:?}", key, value);
        thread::sleep(Duration::from_millis(500));
    }
    // bucket.delete_to(end_key, true).unwrap();

    let bucket_size = match fs::metadata(file_path) {
        Ok(metadata) => metadata.len(),
        Err(_) => 0,
    };
    println!("Bucket file size {} bytes", bucket_size);
}
```
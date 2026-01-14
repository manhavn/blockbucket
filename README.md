# blockbucket

**blockbucket** is a tiny file-backed key-value bucket (binary `Vec<u8>` key/value) with simple operations:

- `set` / `get` / `delete`
- `set_many`
- `list` / `list_next` (pagination)
- `find_next`
- `delete_to`
- `list_lock_delete` (queue-like pop)

Storage is backed by a **single file** (example: `data.db`).

> ⚠️ Note: This crate is intentionally simple.
> It is not a transactional database and does not guarantee crash-safety or durability guarantees like a real DB.

---

## Install

```bash
 cargo add blockbucket
```

Or:

```toml
[dependencies]
blockbucket = "0.2.8"
```

---

## Quick start

```rust
use blockbucket::{Bucket, Trait};

fn main() -> std::io::Result<()> {
    let mut bucket = Bucket::new("data.db".to_string())?;

    let key = b"test-key-001".to_vec();
    let value = b"hello blockbucket".to_vec();

    bucket.set(key.clone(), value.clone())?;

    let (k, v) = bucket.get(key.clone());
    assert_eq!(k, key);
    assert_eq!(v, value);

    bucket.delete(key)?;
    Ok(())
}
```

---

## Batch write (set_many)

```rust
use blockbucket::{Bucket, Trait};

fn main() -> std::io::Result<()> {
    let mut bucket = Bucket::new("data.db".to_string())?;

    let mut items = Vec::new();
    for i in 0..10 {
        let key = format!("k{:02}", i).into_bytes();
        let value = format!("value-{}", i).into_bytes();
        items.push((key, value));
    }

    bucket.set_many(items)?;
    Ok(())
}
```

---

## Listing & pagination

list(limit)

```rust
use blockbucket::{Bucket, Trait};

fn main() {
    let mut bucket = Bucket::new("data.db".to_string()).unwrap();
    let rows = bucket.list(10);
    println!("rows={}", rows.len());
}
```

list_next(limit, skip)

```rust
use blockbucket::{Bucket, Trait};

fn main() {
    let mut bucket = Bucket::new("data.db".to_string()).unwrap();

    let page1 = bucket.list_next(10, 0);
    let page2 = bucket.list_next(10, 10);

    println!("page1={}, page2={}", page1.len(), page2.len());
}
```

---

## find_next(key, limit, only_after_key)

Find a window of items around a given key:

- `only_after_key = false`: include the found key
- `only_after_key = true`: return items after the found key

```rust
use blockbucket::{Bucket, Trait};

fn main() {
    let mut bucket = Bucket::new("data.db".to_string()).unwrap();

    let rows = bucket.find_next(b"test-key-001".to_vec(), 10, false);
    println!("found={}", rows.len());
}
```

---

## delete_to(key, also_delete_the_found_block)

Delete items up to a key:

- `also_delete_the_found_block = true`: include the key block itself
- `also_delete_the_found_block = false`: keep the found key and delete items before it

```rust
use blockbucket::{Bucket, Trait};

fn main() -> std::io::Result<()> {
    let mut bucket = Bucket::new("data.db".to_string())?;
    bucket.delete_to(b"test-key-001".to_vec(), true)?;
    Ok(())
}
```

---

## list_lock_delete(limit)

Queue-like behavior: read up to `limit` items and delete them.

```rust
use blockbucket::{Bucket, Trait};

fn main() -> std::io::Result<()> {
    let mut bucket = Bucket::new("data.db".to_string())?;
    let popped = bucket.list_lock_delete(10)?;
    println!("popped={}", popped.len());
    Ok(())
}
```

---

## Behavior notes

- `get(key)` returns `(Vec::new(), Vec::new())` when key is not found.
- Keys and values are stored as raw bytes.
- Operations are file-backed (single file).

---

## Test

`test.rs`

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

---

## DEMO queue app

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
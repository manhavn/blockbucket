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
blockbucket = "0.2.3" # https://crates.io/crates/blockbucket
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

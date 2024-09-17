pub mod template_impl;

#[cfg(test)]
mod tests {
    use std::{
        cmp::Ordering as CmpOrdering,
        mem,
        ops::{Deref, DerefMut},
        str,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc, Mutex,
        },
        thread,
        time::Duration,
    };

    use log::LevelFilter;
    use rand::{distributions::Alphanumeric, thread_rng, Rng};

    use super::template_impl::TemplateDB;
    use crate::{
        cache::{bloom_filter_cache::BloomFilter, lru_cache::LRUCache},
        db_trait::DB,
        error::{TemplateKVError, TemplateResult},
        iterator::Iterator,
        manager::{
            filename::{parse_filename, FileType},
            snapshot::Snapshot,
        },
        memtable::{
            key_format::{InternalKey, ParsedInternalKey},
            value_format::ValueType,
        },
        options::{CompressionType, Options, ReadOptions, WriteOptions, MAX_KEY_SEQUENCE},
        storage::{mem::MemStorage, Storage},
        util::comparator::{BytewiseComparator, Comparator},
    };

    impl<S: Storage + Clone, C: Comparator + 'static> TemplateDB<S, C> {
        fn options(&self) -> Arc<Options<C>> {
            self.inner.options.clone()
        }

        fn total_sst_files(&self) -> usize {
            let versions = self.inner.versions.lock().unwrap();
            let mut res = 0;
            for l in 0..self.options().max_levels {
                res += versions.level_files_count(l);
            }
            res
        }

        fn file_count_per_level(&self) -> String {
            let mut res = String::new();
            let versions = self.inner.versions.lock().unwrap();
            for l in 0..self.options().max_levels {
                let count = versions.level_files_count(l);
                res.push_str(&count.to_string());
                res.push(',');
            }
            res.trim_end_matches("0,").trim_end_matches(',').to_owned()
        }
    }

    #[derive(Debug, Clone, Copy, FromPrimitive)]
    enum TestOption {
        Default,
        // Enable `reuse_log`
        Reuse,
        // Use Bloom Filter as the filter policy
        FilterPolicy,
        // No compression enabled
        UnCompressed,
    }

    impl From<u8> for TestOption {
        fn from(src: u8) -> TestOption {
            num_traits::FromPrimitive::from_u8(src).unwrap()
        }
    }

    fn new_test_options(o: TestOption) -> Options<BytewiseComparator> {
        match o {
            TestOption::Default => Options::default(),
            TestOption::Reuse => {
                let mut o = Options::default();
                o.reuse_logs = true;
                o
            }
            TestOption::FilterPolicy => {
                let filter = BloomFilter::new(10);
                let mut o = Options::default();
                o.filter_policy = Some(Arc::new(filter));
                o
            }
            TestOption::UnCompressed => {
                let mut o = Options::default();
                o.compression = CompressionType::NoCompression;
                o
            }
        }
    }

    fn iter_to_string(iter: &dyn Iterator) -> String {
        if iter.valid() {
            format!(
                "{}->{}",
                str::from_utf8(iter.key()).unwrap(),
                str::from_utf8(iter.value()).unwrap()
            )
        } else {
            "(invalid)".to_owned()
        }
    }

    fn default_cases() -> Vec<DBTest> {
        cases(|opt| opt)
    }

    fn cases<F>(mut opt_hook: F) -> Vec<DBTest>
    where
        F: FnMut(Options<BytewiseComparator>) -> Options<BytewiseComparator>,
    {
        vec![
            TestOption::Default,
            TestOption::Reuse,
            TestOption::FilterPolicy,
            TestOption::UnCompressed,
        ]
        .into_iter()
        .map(|opt| {
            let options = opt_hook(new_test_options(opt));
            DBTest::new(options)
        })
        .collect()
    }

    struct DBTest {
        // Used as the db's inner storage
        store: MemStorage,
        // Used as the db's options
        opt: Options<BytewiseComparator>,
        db: TemplateDB<MemStorage, BytewiseComparator>,
    }

    #[allow(dead_code)]
    impl DBTest {
        fn new(opt: Options<BytewiseComparator>) -> Self {
            let store = MemStorage::default();
            let name = "db_test";
            let db = TemplateDB::open_db(opt.clone(), name, store.clone()).unwrap();
            DBTest { store, opt, db }
        }

        // Close the inner db without destroy the contents and establish a new TemplateDB on same db
        // path with same option
        fn reopen(&mut self) -> TemplateResult<()> {
            self.db.close()?;
            let db =
                TemplateDB::open_db(self.opt.clone(), &self.db.inner.db_path, self.store.clone())?;
            self.db = db;
            Ok(())
        }

        // Put entries with default `WriteOptions`
        fn put_entries(&self, entries: Vec<(&str, &str)>) {
            for (k, v) in entries {
                self.db
                    .put(WriteOptions::default(), k.as_bytes(), v.as_bytes())
                    .unwrap()
            }
        }

        fn put(&self, k: &str, v: &str) -> TemplateResult<()> {
            self.db
                .put(WriteOptions::default(), k.as_bytes(), v.as_bytes())
        }

        fn delete(&self, k: &str) -> TemplateResult<()> {
            self.db.delete(WriteOptions::default(), k.as_bytes())
        }

        fn get(&self, k: &str, snapshot: Option<Snapshot>) -> Option<String> {
            let mut read_opt = ReadOptions::default();
            read_opt.snapshot = snapshot;
            match self.db.get(read_opt, k.as_bytes()) {
                Ok(v) => v.map(|v| unsafe { String::from_utf8_unchecked(v) }),
                Err(_) => None,
            }
        }
        fn assert_get(&self, k: &str, expect: Option<&str>) {
            match self.db.get(ReadOptions::default(), k.as_bytes()) {
                Ok(v) => match v {
                    Some(s) => {
                        let bytes = s.as_slice();
                        let expect = expect.map(|s| s.as_bytes());
                        if bytes.len() > 1000 {
                            if expect != Some(bytes) {
                                panic!("expect(len={}), but got(len={}), not equal contents, key: {}, got: {:?}..., expect: {:?}...", expect.map_or(0, |s| s.len()), bytes.len(), k, &bytes[..50], &expect.unwrap()[..50]);
                            }
                        } else {
                            assert_eq!(expect, Some(bytes), "key: {}", k);
                        }
                    }
                    None => assert_eq!(expect, None, "key: {}", k),
                },
                Err(e) => panic!("got error {:?}, key: {}", e, k),
            }
        }

        fn must_release_snapshot(&self, s: Arc<Snapshot>) {
            assert!(self.release_snapshot(s))
        }

        // Return a string that contains all key,value pairs in order,
        // formatted like "(k1->v1)(k2->v2)...".
        // Also checks the db iterator works fine in both forward and backward direction
        fn assert_contents(&self) -> String {
            let mut iter = self.db.iter(ReadOptions::default()).unwrap();
            iter.seek_to_first();
            let mut result = String::new();
            let mut backward = vec![];
            while iter.valid() {
                let s = iter_to_string(&iter);
                result.push('(');
                result.push_str(&s);
                result.push(')');
                backward.push(s);
                iter.next();
            }

            // Chech reverse iteration results are reverse of forward results
            backward.reverse();
            iter.seek_to_last();
            let mut matched = 0;
            while iter.valid() {
                assert!(matched < backward.len());
                assert_eq!(iter_to_string(&iter), backward[matched]);
                iter.prev();
                matched += 1
            }
            assert_eq!(matched, backward.len());
            result
        }

        // Return all the values for the given `user_key`
        fn all_entires_for(&self, user_key: &[u8]) -> String {
            let mut iter = self.db.internal_iter(ReadOptions::default()).unwrap();
            let ikey = InternalKey::new(user_key, MAX_KEY_SEQUENCE, ValueType::Value);
            iter.seek(ikey.data());
            let mut result = String::new();
            if iter.valid() {
                result.push('[');
                let mut first = true;
                while iter.valid() {
                    match ParsedInternalKey::decode_from(iter.key()) {
                        None => result.push_str("CORRUPTED"),
                        Some(pkey) => {
                            if self
                                .db
                                .options()
                                .comparator
                                .compare(pkey.user_key, user_key)
                                != CmpOrdering::Equal
                            {
                                break;
                            }
                            if !first {
                                result.push_str(", ");
                            }
                            first = false;
                            match pkey.value_type {
                                ValueType::Value => {
                                    result.push_str(str::from_utf8(iter.value()).unwrap())
                                }
                                ValueType::Deletion => result.push_str("DEL"),
                                ValueType::Unknown => result.push_str("UNKNOWN"),
                            }
                        }
                    }
                    iter.next();
                }
                if !first {
                    result.push(' ');
                }
                result.push(']');
            } else {
                result = iter.status().unwrap_err().to_string();
            }
            result
        }

        fn compact(&self, begin: Option<&str>, end: Option<&str>) {
            self.db
                .inner
                .compact_range(begin.map(|s| s.as_bytes()), end.map(|s| s.as_bytes()))
                .unwrap()
        }

        // Do `n` memtable compactions, each of which produces an sstable
        // covering the key range `[begin,end]`.
        fn make_sst_files(&self, n: usize, begin: &str, end: &str) {
            for _ in 0..n {
                self.put(begin, "begin").unwrap();
                self.put(end, "end").unwrap();
                self.db.inner.force_compact_mem_table().unwrap();
            }
        }

        // Prevent pushing of new sstables into deeper levels by adding
        // tables that cover a specified range to all levels
        fn fill_levels(&self, begin: &str, end: &str) {
            self.make_sst_files(self.db.options().max_levels, begin, end)
        }

        fn assert_put_get(&self, key: &str, value: &str) {
            self.put(key, value).unwrap();
            assert_eq!(value, self.get(key, None).unwrap());
        }

        fn num_sst_files_at_level(&self, level: usize) -> usize {
            self.inner.versions.lock().unwrap().level_files_count(level)
        }

        // Check the number of sst files at `level` in current version
        fn assert_file_num_at_level(&self, level: usize, expect: usize) {
            assert_eq!(self.num_sst_files_at_level(level), expect);
        }

        // Check all the number of sst files at each level in current version
        fn assert_file_num_at_each_level(&self, expect: Vec<usize>) {
            let current = self.inner.versions.lock().unwrap().current();
            let max_level = self.options().max_levels;
            let mut got = Vec::with_capacity(max_level);
            for l in 0..max_level {
                got.push(current.get_level_files(l).len());
            }
            assert_eq!(got, expect);
        }

        // Print all sst files at current version
        fn print_sst_files(&self) {
            let current = self.inner.versions.lock().unwrap().current();
            println!("{:?}", current);
        }

        fn assert_approximate_size(&self, start: &str, end: &str, a: usize, b: usize) {
            let s = self
                .inner
                .get_approximate_size(start.as_bytes(), end.as_bytes());
            assert!(
                s <= b as u64 && s >= a as u64,
                "approximate size between '{}' - '{}' should be between [{}, {}], but got {}",
                start,
                end,
                a,
                b,
                s
            );
        }

        // Delete a sst file randomly
        fn delete_one_sst_file(&self) -> TemplateResult<bool> {
            let files = self.store.list(&self.inner.db_path)?;
            for f in files {
                if let Some((tp, _)) = parse_filename(&f) {
                    if tp == FileType::Table {
                        self.store.remove(&f)?;
                        return Ok(true);
                    }
                }
            }
            Ok(false)
        }
    }

    impl Default for DBTest {
        fn default() -> Self {
            let store = MemStorage::default();
            let name = "db_test";
            let opt = new_test_options(TestOption::Default);
            let db = TemplateDB::open_db(opt.clone(), name, store.clone()).unwrap();
            DBTest { store, opt, db }
        }
    }

    impl Deref for DBTest {
        type Target = TemplateDB<MemStorage, BytewiseComparator>;
        fn deref(&self) -> &Self::Target {
            &self.db
        }
    }

    impl DerefMut for DBTest {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.db
        }
    }

    fn rand_string(n: usize) -> String {
        thread_rng()
            .sample_iter(&Alphanumeric)
            .take(n)
            .collect::<String>()
    }

    fn key(i: usize) -> String {
        format!("key{:06}", i)
    }

    #[test]
    fn test_empty_db() {
        for t in default_cases() {
            assert_eq!(None, t.get("foo", None))
        }
    }

    #[test]
    fn test_empty_key() {
        for t in default_cases() {
            t.assert_put_get("", "v1");
            t.assert_put_get("", "v2");
        }
    }

    #[test]
    fn test_empty_value() {
        for t in default_cases() {
            t.assert_put_get("key", "v1");
            t.assert_put_get("key", "");
            t.assert_put_get("key", "v2");
        }
    }

    #[test]
    fn test_read_write() {
        for t in default_cases() {
            t.assert_put_get("foo", "v1");
            t.put("bar", "v2").unwrap();
            t.put("foo", "v3").unwrap();
            assert_eq!("v3", t.get("foo", None).unwrap());
            assert_eq!("v2", t.get("bar", None).unwrap());
        }
    }

    #[test]
    fn test_put_delete_get() {
        for t in default_cases() {
            t.assert_put_get("foo", "v1");
            t.assert_put_get("foo", "v2");
            t.delete("foo").unwrap();
            assert_eq!(None, t.get("foo", None));
        }
    }

    #[test]
    // Test getting kv from immutable memtable and SSTable
    fn test_get_from_immutable_layer() {
        for t in cases(|mut opt| {
            opt.write_buffer_size = 100_000; // Small write buffer
            opt
        }) {
            t.assert_put_get("foo", "v1");
            // block `flush()`
            t.store.delay_data_sync.store(true, Ordering::Release);
            t.put("k1", &"x".repeat(100_000)).unwrap(); // fill memtable
            assert_eq!("v1", t.get("foo", None).unwrap()); // "v1" on immutable table
            t.put("k2", &"y".repeat(100_000)).unwrap(); // trigger compaction
                                                        // Waiting for compaction finish
            thread::sleep(Duration::from_secs(2));
            t.assert_file_num_at_level(2, 1);
            // Try to retrieve key "foo" from level 0 files
            t.assert_get("k1", Some(&"x".repeat(100_000)));
            assert_eq!("v1", t.get("foo", None).unwrap()); // "v1" on SST files
            t.assert_get("k2", Some(&"y".repeat(100_000)));
        }
    }

    #[test]
    // Test `force_compact_mem_table` and kv look up after compaction
    fn test_get_from_versions() {
        for t in default_cases() {
            t.assert_put_get("foo", "v1");
            t.inner.force_compact_mem_table().unwrap();
            assert_eq!("v1", t.get("foo", None).unwrap());
        }
    }

    #[test]
    // Test look up key with snapshot
    fn test_get_with_snapshot() {
        for t in default_cases() {
            for key in [String::from("foo"), "x".repeat(20)] {
                t.assert_put_get(&key, "v1");
                let s = t.db.snapshot();
                t.put(&key, "v2").unwrap();
                assert_eq!(t.get(&key, None).unwrap(), "v2");
                assert_eq!(t.get(&key, Some(s.sequence().into())).unwrap(), "v1");
                t.inner.force_compact_mem_table().unwrap();
                assert_eq!(t.get(&key, None).unwrap(), "v2");
                assert_eq!(t.get(&key, Some(s.sequence().into())).unwrap(), "v1");
            }
        }
    }

    // Ensure `get` returns same result with the same snapshot and the same key
    #[test]
    fn test_get_with_identical_snapshots() {
        for t in default_cases() {
            let keys = vec![String::from("foo"), "x".repeat(200)];
            for key in keys {
                t.assert_put_get(&key, "v1");
                let s1 = t.snapshot();
                let s2 = t.snapshot();
                let s3 = t.snapshot();
                t.assert_put_get(&key, "v2");
                assert_eq!(t.get(&key, Some(s1.sequence().into())).unwrap(), "v1");
                assert_eq!(t.get(&key, Some(s2.sequence().into())).unwrap(), "v1");
                assert_eq!(t.get(&key, Some(s3.sequence().into())).unwrap(), "v1");
                mem::drop(s1);
                t.inner.force_compact_mem_table().unwrap();
                assert_eq!(t.get(&key, None).unwrap(), "v2");
                assert_eq!(t.get(&key, Some(s2.sequence().into())).unwrap(), "v1");
                mem::drop(s2);
                assert_eq!(t.get(&key, Some(s3.sequence().into())).unwrap(), "v1");
            }
        }
    }

    #[test]
    fn test_iterate_over_empty_snapshot() {
        for t in default_cases() {
            let s = t.snapshot();
            let mut read_opt = ReadOptions::default();
            read_opt.snapshot = Some(s.sequence().into());
            t.put("foo", "v1").unwrap();
            t.put("foo", "v2").unwrap();
            let mut iter = t.iter(read_opt).unwrap();
            iter.seek_to_first();
            // No entry at this snapshot
            assert!(!iter.valid());
            // flush entries into sst file
            t.inner.force_compact_mem_table().unwrap();
            let mut iter = t.iter(read_opt).unwrap();
            iter.seek_to_first();
            assert!(!iter.valid());
        }
    }

    // Test that "get" always retrieve entries from the right sst file
    #[test]
    fn test_get_level_0_ordering() {
        for t in default_cases() {
            t.put("bar", "b").unwrap();
            t.put("foo", "v1").unwrap();
            t.inner.force_compact_mem_table().unwrap();
            t.assert_file_num_at_level(2, 1);
            t.put("foo", "v2").unwrap();
            t.inner.force_compact_mem_table().unwrap();
            // The 2nd sst file is placed at level1 because the key "foo" is overlapped with
            // sst file in level 2 (produced by last "force_compact_mem_table")
            t.assert_file_num_at_each_level(vec![0, 1, 1, 0, 0, 0, 0]);
            assert_eq!(t.get("foo", None).unwrap(), "v2");
        }
    }

    #[test]
    fn test_get_ordered_by_levels() {
        for t in default_cases() {
            t.put("foo", "v1").unwrap();
            t.compact(Some("a"), Some("z"));
            assert_eq!(t.get("foo", None).unwrap(), "v1");
            t.put("foo", "v2").unwrap();
            t.inner.force_compact_mem_table().unwrap();
            assert_eq!(t.get("foo", None).unwrap(), "v2");
        }
    }

    #[test]
    fn test_pick_correct_file() {
        for t in default_cases() {
            t.put("a", "va").unwrap();
            t.compact(Some("a"), Some("b"));
            t.put("x", "vx").unwrap();
            t.compact(Some("x"), Some("y"));
            t.put("f", "vf").unwrap();
            t.compact(Some("f"), Some("g"));
            // Each sst file's key range doesn't overlap. So all the sst files are
            // placed at level 2
            t.assert_file_num_at_level(2, 3);
            t.print_sst_files();
            assert_eq!(t.get("a", None).unwrap(), "va");
            assert_eq!(t.get("x", None).unwrap(), "vx");
            assert_eq!(t.get("f", None).unwrap(), "vf");
        }
    }

    #[test]
    fn test_get_encounters_empty_level() {
        for t in default_cases() {
            // Arrange for the following to happen:
            //   * sstable A in level 0
            //   * nothing in level 1
            //   * sstable B in level 2
            // Then do enough Get() calls to arrange for an automatic compaction
            // of sstable A.  A bug would cause the compaction to be marked as
            // occurring at level 1 (instead of the correct level 0).

            // Step 1: First place sstables in levels 0 and 2
            while t.num_sst_files_at_level(0) == 0 || t.num_sst_files_at_level(2) == 0 {
                t.put("a", "begin").unwrap();
                t.put("z", "end").unwrap();
                t.inner.force_compact_mem_table().unwrap();
            }
            t.assert_file_num_at_level(0, 1);
            t.assert_file_num_at_level(1, 1);
            t.assert_file_num_at_level(2, 1);

            // Read a bunch of times to trigger compaction (drain `allow_seek`)
            for _ in 0..1000 {
                assert_eq!(t.get("missing", None), None);
            }
            // Wait for compaction to finish
            thread::sleep(Duration::from_secs(1));
            t.assert_file_num_at_level(0, 0);
            t.assert_file_num_at_level(1, 0);
            t.assert_file_num_at_level(2, 1);
        }
    }

    #[test]
    fn test_iter_empty_db() {
        let t = DBTest::default();
        let mut iter = t.iter(ReadOptions::default()).unwrap();
        iter.seek_to_first();
        assert!(!iter.valid());
        iter.seek_to_last();
        assert!(!iter.valid());
        iter.seek(b"foo");
        assert!(!iter.valid());
    }

    fn assert_iter_entry(iter: &dyn Iterator, k: &str, v: &str) {
        assert_eq!(str::from_utf8(iter.key()).unwrap(), k);
        assert_eq!(str::from_utf8(iter.value()).unwrap(), v);
    }

    #[test]
    fn test_iter_single() {
        let t = DBTest::default();
        t.put("a", "va").unwrap();
        let mut iter = t.iter(ReadOptions::default()).unwrap();
        iter.seek_to_first();
        assert_iter_entry(&iter, "a", "va");
        iter.next();
        assert!(!iter.valid());
        iter.seek_to_first();
        assert_iter_entry(&iter, "a", "va");
        iter.prev();
        assert!(!iter.valid());

        iter.seek_to_last();
        assert_iter_entry(&iter, "a", "va");
        iter.next();
        assert!(!iter.valid());
        iter.seek_to_last();
        assert_iter_entry(&iter, "a", "va");
        iter.prev();
        assert!(!iter.valid());

        iter.seek(b"");
        assert_iter_entry(&iter, "a", "va");
        iter.next();
        assert!(!iter.valid());

        iter.seek(b"a");
        assert_iter_entry(&iter, "a", "va");
        iter.next();
        assert!(!iter.valid());

        iter.seek(b"b");
        assert!(!iter.valid());
    }

    #[test]
    fn test_iter_multi() {
        let t = DBTest::default();
        t.put_entries(vec![("a", "va"), ("b", "vb"), ("c", "vc")]);

        let mut iter = t.iter(ReadOptions::default()).unwrap();
        iter.seek_to_first();
        assert_iter_entry(&iter, "a", "va");
        iter.next();
        assert_iter_entry(&iter, "b", "vb");
        iter.next();
        assert_iter_entry(&iter, "c", "vc");
        iter.next();
        assert!(!iter.valid());
        iter.seek_to_first();
        assert_iter_entry(&iter, "a", "va");
        iter.prev();
        assert!(!iter.valid());

        iter.seek_to_last();
        assert_iter_entry(&iter, "c", "vc");
        iter.prev();
        assert_iter_entry(&iter, "b", "vb");
        iter.prev();
        assert_iter_entry(&iter, "a", "va");
        iter.prev();
        assert!(!iter.valid());
        iter.seek_to_last();
        assert_iter_entry(&iter, "c", "vc");
        iter.next();
        assert!(!iter.valid());

        iter.seek(b"");
        assert_iter_entry(&iter, "a", "va");
        iter.seek(b"a");
        assert_iter_entry(&iter, "a", "va");
        iter.seek(b"ax");
        assert_iter_entry(&iter, "b", "vb");
        iter.seek(b"b");
        assert_iter_entry(&iter, "b", "vb");
        iter.seek(b"z");
        assert!(!iter.valid());

        // Switch from reverse to forward
        iter.seek_to_last();
        iter.prev();
        iter.prev();
        iter.next();
        assert_iter_entry(&iter, "b", "vb");

        // Switch from forward to reverse
        iter.seek_to_first();
        iter.next();
        iter.next();
        iter.prev();
        assert_iter_entry(&iter, "b", "vb");

        // Make sure iter stays at snapshot
        t.put_entries(vec![
            ("a", "va2"),
            ("a2", "va3"),
            ("b", "vb2"),
            ("c", "vc2"),
        ]);
        t.delete("b").unwrap();
        iter.seek_to_first();
        assert_iter_entry(&iter, "a", "va");
        iter.next();
        assert_iter_entry(&iter, "b", "vb");
        iter.next();
        assert_iter_entry(&iter, "c", "vc");
        iter.next();
        assert!(!iter.valid());
        iter.seek_to_last();
        assert_iter_entry(&iter, "c", "vc");
        iter.prev();
        assert_iter_entry(&iter, "b", "vb");
        iter.prev();
        assert_iter_entry(&iter, "a", "va");
        iter.prev();
        assert!(!iter.valid());
    }

    #[test]
    fn test_iter_small_and_large_mix() {
        let t = DBTest::default();
        let count = 100_000;
        t.put_entries(vec![
            ("a", "va"),
            ("b", &"b".repeat(count)),
            ("c", "vc"),
            ("d", &"d".repeat(count)),
            ("e", &"e".repeat(count)),
        ]);
        let mut iter = t.iter(ReadOptions::default()).unwrap();

        iter.seek_to_first();
        assert_iter_entry(&iter, "a", "va");
        iter.next();
        assert_iter_entry(&iter, "b", &"b".repeat(count));
        iter.next();
        assert_iter_entry(&iter, "c", "vc");
        iter.next();
        assert_iter_entry(&iter, "d", &"d".repeat(count));
        iter.next();
        assert_iter_entry(&iter, "e", &"e".repeat(count));
        iter.next();
        assert!(!iter.valid());

        iter.seek_to_last();
        assert_iter_entry(&iter, "e", &"e".repeat(count));
        iter.prev();
        assert_iter_entry(&iter, "d", &"d".repeat(count));
        iter.prev();
        assert_iter_entry(&iter, "c", "vc");
        iter.prev();
        assert_iter_entry(&iter, "b", &"b".repeat(count));
        iter.prev();
        assert_iter_entry(&iter, "a", "va");
        iter.prev();
        assert!(!iter.valid());
    }

    #[test]
    fn test_iter_multi_with_delete() {
        for t in default_cases() {
            t.put_entries(vec![("a", "va"), ("b", "vb"), ("c", "vc")]);
            t.delete("b").unwrap();
            assert_eq!(t.get("b", None), None);
            let mut iter = t.iter(ReadOptions::default()).unwrap();
            iter.seek(b"c");
            assert_iter_entry(&iter, "c", "vc");
            iter.prev();
            assert_iter_entry(&iter, "a", "va");
        }
    }

    #[test]
    fn test_iter_pins_ref() {
        let t = DBTest::default();
        t.put("foo", "hello").unwrap();

        // Get iterator that will yield the current contents of the DB.
        let mut iter = t.iter(ReadOptions::default()).unwrap();

        // Wirte to force compactions
        t.put("foo", "newvalue1").unwrap();
        for i in 0..100 {
            t.put(&key(i), &(key(i) + "v".repeat(100_000).as_str()))
                .unwrap();
        }
        t.put("foo", "newvalue2").unwrap();
        iter.seek_to_first();
        assert!(iter.valid());
        assert_eq!(str::from_utf8(iter.key()).unwrap(), "foo");
        assert_eq!(str::from_utf8(iter.value()).unwrap(), "hello");
        iter.next();
        // Iter should only contains entries before being created
        assert!(!iter.valid());
    }

    // #[test]
    // fn test_reopen_with_empty_db() {
    //     for mut t in default_cases() {
    //         t.reopen().unwrap();
    //         t.reopen().unwrap();

    //         t.put_entries(vec![("foo", "v1"), ("foo", "v2")]);
    //         t.reopen().unwrap();
    //         t.reopen().unwrap();
    //         t.put("foo", "v3").unwrap();
    //         t.reopen().unwrap();
    //         assert_eq!(t.get("foo", None).unwrap(), "v3");
    //     }
    // }

    // #[test]
    // fn test_recover_with_entries() {
    //     for mut t in default_cases() {
    //         t.put_entries(vec![("foo", "v1"), ("baz", "v5")]);
    //         t.reopen().unwrap();
    //         assert_eq!(t.get("foo", None).unwrap(), "v1");
    //         assert_eq!(t.get("baz", None).unwrap(), "v5");

    //         t.put_entries(vec![("bar", "v2"), ("foo", "v3")]);
    //         t.reopen().unwrap();
    //         assert_eq!(t.get("foo", None).unwrap(), "v3");
    //         t.put("foo", "v4").unwrap();
    //         assert_eq!(t.get("bar", None).unwrap(), "v2");
    //         assert_eq!(t.get("foo", None).unwrap(), "v4");
    //         assert_eq!(t.get("baz", None).unwrap(), "v5");
    //     }
    // }

    // Check that writes done during a memtable compaction are recovered
    // if the database is shutdown during the memtable compaction.
    // #[test]
    // fn test_recover_during_memtable_compaction() {
    //     for mut t in cases(|mut opt| {
    //         opt.write_buffer_size = 10000;
    //         opt
    //     }) {
    //         // Trigger a long memtable compaction and reopen the database during it
    //         t.put_entries(vec![
    //             ("foo", "v1"),                             // Goes to 1st log file
    //             ("big1", "x".repeat(10_000_000).as_str()), // Fills memtable
    //             ("big2", "y".repeat(1000).as_str()),       // Triggers compaction
    //             ("bar", "v2"),                             // Goes to new log file
    //         ]);
    //         t.reopen().unwrap();
    //         t.assert_get("foo", Some("v1"));
    //         t.assert_get("bar", Some("v2"));
    //         t.assert_get("big1", Some("x".repeat(10_000_000).as_str()));
    //         t.assert_get("big2", Some("y".repeat(1000).as_str()));
    //     }
    // }

    // #[test]
    // fn test_minor_compactions_happend() {
    //     let mut opts = Options::default();
    //     opts.write_buffer_size = 10000;
    //     let mut t = DBTest::new(opts);
    //     let n = 500;
    //     let starting_num_tables = t.total_sst_files();
    //     for i in 0..n {
    //         t.put(&key(i), &(key(i) + "v".repeat(1000).as_str()))
    //             .unwrap();
    //     }
    //     let ending_num_tables = t.total_sst_files();
    //     assert!(starting_num_tables < ending_num_tables);
    //     for i in 0..n {
    //         t.assert_get(&key(i), Some(&(key(i) + "v".repeat(1000).as_str())))
    //     }
    //     t.reopen().unwrap();
    //     for i in 0..n {
    //         t.assert_get(&key(i), Some(&(key(i) + "v".repeat(1000).as_str())))
    //     }
    // }

    #[test]
    fn test_recover_with_large_log() {
        let opts = Options::default();
        let mut t = DBTest::new(opts);
        t.put("big1", &"1".repeat(200_000)).unwrap();
        t.put("big2", &"2".repeat(200_000)).unwrap();
        t.put("small3", &"3".repeat(10)).unwrap();
        t.put("small4", &"4".repeat(10)).unwrap();
        assert_eq!(t.num_sst_files_at_level(0), 0);

        // Make sure that if we re-open with a small write buffer size that
        // we flush table files in the middle of a large log file.
        t.opt.write_buffer_size = 100_000;
        t.reopen().unwrap();
        assert_eq!(t.num_sst_files_at_level(0), 3);
        t.assert_get("big1", Some(&"1".repeat(200_000)));
        t.assert_get("big2", Some(&"2".repeat(200_000)));
        t.assert_get("small3", Some(&"3".repeat(10)));
        t.assert_get("small4", Some(&"4".repeat(10)));
    }

    // TODO(luhuanbing): fix this
    // #[test]
    // fn test_compaction_generate_multiple_files() {
    //     let mut opt = Options::default();
    //     opt.write_buffer_size = 100_000_000;
    //     let mut t = DBTest::new(opt);
    //     t.assert_file_num_at_level(0, 0);
    //     let n = 80;
    //     // write 8MB (80 values, each 100k)
    //     let mut values = vec![];
    //     for i in 0..n {
    //         let v = rand_string(100_000);
    //         values.push(v.clone());
    //         t.put(&i.to_string(), &v).unwrap();
    //     }

    //     // As opt.reuse_log = false, reopening moves entries into level-0 after replaying the WAL
    //     t.reopen().unwrap();
    //     for i in 0..n {
    //         t.assert_get(&i.to_string(), Some(&values[i]));
    //     }
    //     t.compact_range_at(0, None, None).unwrap();
    //     t.assert_file_num_at_level(0, 0);
    //     let l1_count = t.inner.versions.lock().unwrap().level_files_count(1);
    //     assert!(
    //         l1_count > 1,
    //         "level 1 file numbers should > 1, but got {}",
    //         l1_count
    //     );
    //     for i in 0..n {
    //         t.assert_get(&i.to_string(), Some(&values[i]));
    //     }
    // }

    #[test]
    fn test_repeated_write_to_same_key() {
        let mut opt = Options::default();
        opt.write_buffer_size = 100_000; // limit the size of memtable
        opt.logger_level = LevelFilter::Trace;
        // We must have at most one file per level except for level-0,
        // which may have up to kL0_StopWritesTrigger files.
        let max_files = opt.l0_stop_writes_threshold + opt.max_levels;
        let t = DBTest::new(opt.clone());
        let v = rand_string(2 * opt.write_buffer_size);
        for i in 0..10 * max_files {
            t.put("key", &v).unwrap();
            assert!(
                t.total_sst_files() < max_files,
                "after {}: {} total files",
                i,
                t.total_sst_files()
            );
        }
    }

    #[test]
    #[ignore]
    fn test_sparse_merge() {
        let mut opt = Options::default();
        opt.compression = CompressionType::NoCompression;
        opt.logger_level = LevelFilter::Trace;
        let t = DBTest::new(opt.clone());
        t.fill_levels("A", "Z");
        // Suppose there is:
        //    small amount of data with prefix A
        //    large amount of data with prefix B
        //    small amount of data with prefix C
        // and that recent updates have made small changes to all three prefixes.
        // Check that we do not do a compaction that merges all of B in one shot.
        t.put("A", "va").unwrap();
        // Write approximately 100MB of "B" values
        for i in 0..100_000 {
            t.put(format!("B{}", i).as_str(), "x".repeat(1000).as_str())
                .unwrap();
        }
        t.put("C", "vc").unwrap();
        t.inner.force_compact_mem_table().unwrap();
        t.compact_range_at(0, None, None).unwrap();

        // Make sparse update
        t.put("A", "va2").unwrap();
        t.put("B100", "bvalue2").unwrap();
        t.put("C", "vc2").unwrap();
        t.inner.force_compact_mem_table().unwrap();

        // Compactions should not cause us to create a situation where
        // a file overlaps too much data at the next level.
        assert!(
            t.inner
                .versions
                .lock()
                .unwrap()
                .max_next_level_overlapping_bytes()
                < 20 * 1024 * 1024
        );
        t.compact_range_at(0, None, None).unwrap();
        assert!(
            t.inner
                .versions
                .lock()
                .unwrap()
                .max_next_level_overlapping_bytes()
                < 20 * 1024 * 1024
        );
        t.compact_range_at(1, None, None).unwrap();
        assert!(
            t.inner
                .versions
                .lock()
                .unwrap()
                .max_next_level_overlapping_bytes()
                < 20 * 1024 * 1024
        );
    }

    // #[test]
    // fn test_approximate_size() {
    //     for mut t in cases(|mut opt| {
    //         opt.write_buffer_size = 100_000_000;
    //         opt.compression = CompressionType::NoCompression;
    //         opt
    //     }) {
    //         t.assert_approximate_size("", "xyz", 0, 0);
    //         t.assert_file_num_at_level(0, 0);
    //         let n = 80;
    //         let s1 = 100_000;
    //         let s2 = 105_000; // allow some expansion from metadata
    //         for i in 0..n {
    //             t.put(&key(i), &rand_string(s1)).unwrap();
    //         }
    //         // approximate_size does not account for memtable
    //         t.assert_approximate_size("", &key(50), 0, 0);
    //         if t.options().reuse_logs {
    //             t.reopen().unwrap();
    //             // Recovery will reuse memtable
    //             t.assert_approximate_size("", &key(50), 0, 0);
    //             continue;
    //         }
    //         // Check sizes across recovery by reopening a few times
    //         for _ in 0..3 {
    //             t.reopen().unwrap();
    //             for compact_start in (0..n).step_by(10) {
    //                 for i in (0..n).step_by(10) {
    //                     t.assert_approximate_size("", &key(i), s1 * i, s2 * i);
    //                     t.assert_approximate_size(
    //                         "",
    //                         &(key(i) + ".suffix"),
    //                         s1 * (i + 1),
    //                         s2 * (i + 1),
    //                     );
    //                     t.assert_approximate_size(&key(i), &key(i + 10), s1 * 10, s2 * 10);
    //                 }
    //                 t.assert_approximate_size("", &key(50), s1 * 50, s2 * 50);
    //                 t.assert_approximate_size("", &(key(50) + ".suffix"), s1 * 50, s2 * 50);
    //                 t.compact_range_at(
    //                     0,
    //                     Some(key(compact_start).as_bytes()),
    //                     Some(key(compact_start + 9).as_bytes()),
    //                 )
    //                 .unwrap();
    //             }
    //             t.assert_file_num_at_level(0, 0);
    //             assert!(t.num_sst_files_at_level(1) > 0);
    //         }
    //     }
    // }

    // #[test]
    // fn test_approximiate_sizes_min_of_small_and_large() {
    //     for mut t in cases(|mut opt| {
    //         opt.compression = CompressionType::NoCompression;
    //         opt
    //     }) {
    //         let big1 = rand_string(100_000);
    //         t.put(&key(0), &rand_string(10000)).unwrap();
    //         t.put(&key(1), &rand_string(10000)).unwrap();
    //         t.put(&key(2), &big1).unwrap();
    //         t.put(&key(3), &rand_string(10000)).unwrap();
    //         t.put(&key(4), &big1).unwrap();
    //         t.put(&key(5), &rand_string(10000)).unwrap();
    //         t.put(&key(6), &rand_string(300_000)).unwrap();
    //         t.put(&key(7), &rand_string(10000)).unwrap();
    //         if t.opt.reuse_logs {
    //             t.inner.force_compact_mem_table().unwrap();
    //         }
    //         for _ in 0..3 {
    //             t.reopen().unwrap();
    //             t.assert_approximate_size("", &key(0), 0, 0);
    //             t.assert_approximate_size("", &key(1), 10000, 11000);
    //             t.assert_approximate_size("", &key(2), 20000, 21000);
    //             t.assert_approximate_size("", &key(3), 120_000, 121_000);
    //             t.assert_approximate_size("", &key(4), 130_000, 131_000);
    //             t.assert_approximate_size("", &key(5), 230_000, 231_000);
    //             t.assert_approximate_size("", &key(6), 240_000, 241_000);
    //             t.assert_approximate_size("", &key(7), 540_000, 541_000);
    //             t.assert_approximate_size("", &key(8), 550_000, 560_000);
    //             t.assert_approximate_size(&key(3), &key(5), 110_000, 111_000);
    //             t.compact_range_at(0, None, None).unwrap();
    //         }
    //     }
    // }

    #[test]
    fn test_snapshot() {
        for t in default_cases() {
            t.put("foo", "v1").unwrap();
            let s1 = t.snapshot();
            t.put("foo", "v2").unwrap();
            let s2 = t.snapshot();
            t.put("foo", "v3").unwrap();
            let s3 = t.snapshot();
            t.put("foo", "v4").unwrap();

            assert_eq!(
                Some("v1".to_owned()),
                t.get("foo", Some(s1.sequence().into()))
            );
            assert_eq!(
                Some("v2".to_owned()),
                t.get("foo", Some(s2.sequence().into()))
            );
            assert_eq!(
                Some("v3".to_owned()),
                t.get("foo", Some(s3.sequence().into()))
            );
            assert_eq!(Some("v4".to_owned()), t.get("foo", None));
            let mut versions = t.inner.versions.lock().unwrap();
            versions.snapshots.gc();
            assert!(!versions.snapshots.is_empty());
            mem::drop(s1);
            mem::drop(s2);
            mem::drop(s3);
            versions.snapshots.gc();
            assert!(versions.snapshots.is_empty());
        }
    }

    // #[test]
    // fn test_hidden_values_are_removed() {
    //     for t in default_cases() {
    //         t.fill_levels("a", "z");
    //         let big = rand_string(50000);
    //         t.put("foo", &big).unwrap();
    //         t.put("pastfoo", "v").unwrap();
    //         let s = t.snapshot();
    //         t.put("foo", "tiny").unwrap();
    //         t.put("pastfoo2", "v2").unwrap();
    //         t.inner.force_compact_mem_table().unwrap();
    //         assert!(t.num_sst_files_at_level(0) > 0);

    //         assert_eq!(t.get("foo", Some(s.sequence().into())), Some(big.clone()));
    //         t.assert_approximate_size("", "pastfoo", 50000, 60000);
    //         t.must_release_snapshot(s);
    //         assert_eq!(format!("[ tiny, {} ]", big), t.all_entires_for(b"foo"));
    //         t.compact_range_at(0, None, Some(b"x")).unwrap();
    //         assert_eq!("[ tiny ]".to_owned(), t.all_entires_for(b"foo"));
    //         t.assert_file_num_at_level(0, 0);
    //         assert!(t.num_sst_files_at_level(1) >= 1);
    //         t.compact_range_at(1, None, Some(b"x")).unwrap();
    //         assert_eq!("[ tiny ]".to_owned(), t.all_entires_for(b"foo"));
    //         t.assert_approximate_size("", "pastfoo", 0, 1000);
    //     }
    // }

    #[test]
    fn test_mem_compact_into_max_level() {
        let t = DBTest::default();
        t.put("foo", "v1").unwrap();
        t.inner.force_compact_mem_table().unwrap();
        t.assert_file_num_at_level(t.opt.max_mem_compact_level, 1);

        // Place a table at level last-1 to prevent merging with preceding mutation
        t.put("a", "begin").unwrap();
        t.put("z", "end").unwrap();
        t.inner.force_compact_mem_table().unwrap();
        t.assert_file_num_at_level(t.opt.max_mem_compact_level, 1);
        t.assert_file_num_at_level(t.opt.max_mem_compact_level - 1, 1);
    }

    // #[test]
    // fn test_deletion_marker1() {
    //     let t = DBTest::default();
    //     t.put("foo", "v1").unwrap();
    //     t.inner.force_compact_mem_table().unwrap();
    //     t.put("a", "begin").unwrap();
    //     t.put("z", "end").unwrap();
    //     t.inner.force_compact_mem_table().unwrap();
    //     t.delete("foo").unwrap();
    //     t.put("foo", "v2").unwrap();
    //     assert_eq!(t.all_entires_for(b"foo"), "[ v2, DEL, v1 ]");
    //     t.inner.force_compact_mem_table().unwrap();
    //     assert_eq!(t.all_entires_for(b"foo"), "[ v2, DEL, v1 ]");
    //     let level = t.opt.max_mem_compact_level; // default is 2
    //     t.compact_range_at(level - 2, None, Some(b"z")).unwrap();
    //     // DELE eliminated, but v1 remains because we aren't compaction that level
    //     assert_eq!(t.all_entires_for(b"foo"), "[ v2, v1 ]");
    //     t.compact_range_at(level - 1, None, None).unwrap();
    //     // Mergeing last-1 with last, so we are the base level for "foo"
    //     assert_eq!(t.all_entires_for(b"foo"), "[ v2 ]");
    // }

    // #[test]
    // fn test_deletion_marker2() {
    //     let t = DBTest::default();
    //     t.put("foo", "v1").unwrap();
    //     t.inner.force_compact_mem_table().unwrap();
    //     t.put("a", "begin").unwrap();
    //     t.put("z", "end").unwrap();
    //     t.inner.force_compact_mem_table().unwrap();

    //     t.delete("foo").unwrap();
    //     assert_eq!(t.all_entires_for(b"foo"), "[ DEL, v1 ]");
    //     t.inner.force_compact_mem_table().unwrap();
    //     assert_eq!(t.all_entires_for(b"foo"), "[ DEL, v1 ]");
    //     let level = t.opt.max_mem_compact_level; // default is 2
    //     t.compact_range_at(level - 2, None, None).unwrap();
    //     assert_eq!(t.all_entires_for(b"foo"), "[ DEL, v1 ]");
    //     t.compact_range_at(level - 1, None, None).unwrap();
    //     assert_eq!(t.all_entires_for(b"foo"), "[ ]");
    // }

    // #[test]
    // fn test_overlap_in_level_0() {
    //     for t in default_cases() {
    //         // Fill levels 1 and 2 to disable the pushing or new memtables to levels > 0
    //         t.put("100", "v100").unwrap();
    //         t.put("999", "v999").unwrap();
    //         t.inner.force_compact_mem_table().unwrap();
    //         t.delete("100").unwrap();
    //         t.delete("999").unwrap();
    //         t.inner.force_compact_mem_table().unwrap();
    //         assert_eq!("0,1,1", t.file_count_per_level());

    //         // Make files spanning the following ranges in level-0:
    //         //  files[0]  200 .. 900
    //         //  files[1]  300 .. 500
    //         // Note that filtes are sorted by smallest key
    //         t.put("300", "v300").unwrap();
    //         t.put("500", "v500").unwrap();
    //         t.inner.force_compact_mem_table().unwrap();
    //         t.put("200", "v200").unwrap();
    //         t.put("600", "v600").unwrap();
    //         t.put("900", "v000").unwrap();
    //         t.inner.force_compact_mem_table().unwrap();
    //         assert_eq!("2,1,1", t.file_count_per_level());

    //         // Compact away the placeholder files we created initially
    //         t.compact_range_at(1, None, None).unwrap();
    //         t.compact_range_at(2, None, None).unwrap();
    //         assert_eq!("2", t.file_count_per_level());

    //         // Do a memtable compaction
    //         t.delete("600").unwrap();
    //         t.inner.force_compact_mem_table().unwrap();
    //         assert_eq!("3", t.file_count_per_level());
    //         t.assert_get("600", None);
    //     }
    // }

    // #[test]
    // fn test_l0_compaction_when_reopen() {
    //     let mut t = DBTest::default();
    //     assert_eq!("", t.assert_contents());
    //     t.put("b", "v").unwrap();
    //     t.reopen().unwrap();
    //     t.delete("b").unwrap();
    //     t.delete("a").unwrap();
    //     t.reopen().unwrap();
    //     t.delete("a").unwrap();
    //     t.reopen().unwrap();
    //     t.put("a", "v").unwrap();
    //     t.reopen().unwrap();
    //     t.reopen().unwrap();
    //     assert_eq!("(a->v)", t.assert_contents());
    //     t.delete("a").unwrap();
    //     t.put("", "").unwrap();
    //     t.delete("e").unwrap();
    //     t.reopen().unwrap();
    //     t.put("c", "cv").unwrap();
    //     t.reopen().unwrap();
    //     t.put("", "").unwrap();
    //     t.reopen().unwrap();
    //     t.put("", "").unwrap();
    //     t.reopen().unwrap();
    //     t.put("d", "dv").unwrap();
    //     t.reopen().unwrap();
    //     t.delete("d").unwrap();
    //     t.delete("b").unwrap();
    //     t.reopen().unwrap();
    //     assert_eq!("(->)(c->cv)", t.assert_contents());
    // }

    #[test]
    fn test_comparator_check() {
        use std::cmp::Ordering;
        #[derive(Clone, Default)]
        struct NewComparator(BytewiseComparator);
        impl Comparator for NewComparator {
            fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
                self.0.compare(a, b)
            }
            fn name(&self) -> &str {
                "TemplateDB.NewComparator"
            }
            fn separator(&self, a: &[u8], b: &[u8]) -> Vec<u8> {
                self.0.separator(a, b)
            }
            fn successor(&self, key: &[u8]) -> Vec<u8> {
                self.0.successor(key)
            }
        }
        let mut opts = Options::default();
        opts.comparator = NewComparator(BytewiseComparator {});
        let storage = MemStorage::default();
        let mut db = TemplateDB::open_db(opts, "test", storage.clone()).unwrap();
        db.close().unwrap();
        let opts = Options::<BytewiseComparator>::default();
        let res = TemplateDB::open_db(opts, "test", storage.clone());
        match res {
            Ok(_) => panic!("should panic"),
            Err(e) => assert!(e.to_string().contains("does not match existing compactor")),
        };
    }

    #[test]
    fn test_custom_comparator() {
        use std::{cmp::Ordering, str};
        #[derive(Clone, Default)]
        struct NumberComparator {}
        fn to_number(n: &[u8]) -> usize {
            usize::from_str_radix(str::from_utf8(n).unwrap(), 16).unwrap()
        }
        impl Comparator for NumberComparator {
            fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
                to_number(a).cmp(&to_number(b))
            }
            fn name(&self) -> &str {
                "test.NumberComparator"
            }
            fn separator(&self, _a: &[u8], b: &[u8]) -> Vec<u8> {
                b.to_vec()
            }
            fn successor(&self, key: &[u8]) -> Vec<u8> {
                key.to_vec()
            }
        }
        let mut opts = Options::default();
        opts.comparator = NumberComparator {};
        opts.create_if_missing = true;
        opts.filter_policy = None;
        opts.write_buffer_size = 1000;
        let store = MemStorage::default();
        let db = TemplateDB::open_db(opts, "test", store).unwrap();
        db.put(WriteOptions::default(), b"a", b"ten").unwrap();
        db.put(WriteOptions::default(), b"14", b"twenty").unwrap();
        for _ in 0..2 {
            assert_eq!(
                Some("ten".as_bytes().to_vec()),
                db.get(ReadOptions::default(), b"a").unwrap()
            );
            assert_eq!(
                Some("twenty".as_bytes().to_vec()),
                db.get(ReadOptions::default(), b"14").unwrap()
            );
            assert_eq!(None, db.get(ReadOptions::default(), b"f").unwrap());
        }
    }

    #[test]
    fn test_manual_compaction() {
        let opts = Options::<BytewiseComparator> {
            logger_level: LevelFilter::Debug,
            ..Default::default()
        };
        let t = DBTest::new(opts);
        t.make_sst_files(3, "p", "q");
        assert_eq!("1,1,1", t.file_count_per_level());

        // Compaction range falls before files
        t.compact(Some(""), Some("c"));
        assert_eq!("1,1,1", t.file_count_per_level());

        // Compaction range falls after files
        t.compact(Some("r"), Some("z"));
        assert_eq!("1,1,1", t.file_count_per_level());

        t.compact(Some("p1"), Some("p9"));
        assert_eq!("0,0,1", t.file_count_per_level());

        // Populate a different range
        t.make_sst_files(3, "c", "e");
        assert_eq!("1,1,2", t.file_count_per_level());

        t.compact(Some("b"), Some("f"));
        assert_eq!("0,0,2", t.file_count_per_level());

        // Compact all
        t.make_sst_files(1, "a", "z");
        assert_eq!("0,1,2", t.file_count_per_level());
        t.compact(None, None);
        assert_eq!("0,0,1", t.file_count_per_level());
    }

    #[test]
    fn test_dbopen_options() {
        let store = MemStorage::default();
        let mut opts = Options::<BytewiseComparator>::default();
        let dbname = "db_options_test";
        // Does not exist, and create_if_missing == false
        opts.create_if_missing = false;
        match TemplateDB::open_db(opts.clone(), dbname, store.clone()) {
            Ok(_) => panic!("create_if_missing false should return error"),
            Err(e) => assert!(e.to_string().contains("does not exist")),
        }

        // Does not exist, and create_if_missing == true
        opts.create_if_missing = true;
        let mut db = TemplateDB::open_db(opts.clone(), dbname, store.clone()).unwrap();
        db.close().unwrap();

        // Does exist, and error_if_exists == true
        opts.create_if_missing = false;
        opts.error_if_exists = true;
        match TemplateDB::open_db(opts.clone(), dbname, store.clone()) {
            Ok(_) => panic!("error_if_exists true should return error"),
            Err(e) => assert!(e.to_string().contains("exists")),
        }

        // Does exist, and error_if_exists == true
        opts.create_if_missing = true;
        opts.error_if_exists = false;
        let _ = TemplateDB::open_db(opts, dbname, store.clone()).unwrap();
    }

    #[test]
    fn test_destroy_empty_dir() {
        let store = MemStorage::default();
        let opts = Options::<BytewiseComparator>::default();
        let dbname = "db_empty_dir";
        let mut db = TemplateDB::open_db(opts, dbname, store.clone()).unwrap();
        assert_eq!(4, store.list(dbname).unwrap().len());
        // clean up dir
        db.destroy().unwrap();
        assert!(!store.exists(dbname));
        assert!(db.destroy().is_err());
        assert!(db.destroy().is_err());
    }

    #[test]
    fn test_db_file_lock() {
        let store = MemStorage::default();
        let opts = Options::<BytewiseComparator>::default();
        let dbname = "db_file_lock";
        let _ = TemplateDB::open_db(opts.clone(), dbname, store.clone()).unwrap();
        match TemplateDB::open_db(opts, dbname, store.clone()) {
            Ok(_) => panic!("should return error try to create an opened db"),
            Err(e) => assert!(e.to_string().contains("Already locked")),
        }
    }

    // #[test]
    // fn test_missing_sstfile() {
    //     let mut t = DBTest::default();
    //     t.put("foo", "bar").unwrap();
    //     t.inner.force_compact_mem_table().unwrap();
    //     t.assert_get("foo", Some("bar"));
    //     t.close().unwrap();
    //     assert!(t.delete_one_sst_file().unwrap());
    //     t.opt.paranoid_checks = true;
    //     match t.reopen() {
    //         Ok(_) => panic!("Should report missing files"),
    //         Err(e) => assert!(e.to_string().contains("missing files")),
    //     }
    // }

    #[test]
    fn test_file_deleted_after_compaction() {
        let t = DBTest::default();
        t.put("foo", "v2").unwrap();
        t.compact(Some("a"), Some("z"));
        let file_counts = t.store.list(&t.inner.db_path).unwrap().len();
        for _ in 0..10 {
            t.put("foo", "v2").unwrap();
            t.compact(Some("a"), Some("z"))
        }
        assert_eq!(t.store.list(&t.inner.db_path).unwrap().len(), file_counts);
    }

    #[test]
    fn test_db_reads_using_bloom_filter() {
        let mut store = MemStorage::default();
        store.count_random_reads = true;
        let mut opts = Options::<BytewiseComparator>::default();
        opts.logger_level = LevelFilter::Debug;
        opts.block_cache = Some(Arc::new(LRUCache::new(0)));
        let db = TemplateDB::open_db(opts, "bloom_filter_test", store.clone()).unwrap();
        // Populate multiple layers
        let n = 10000;
        for i in 0..n {
            db.put(
                WriteOptions::default(),
                key(i).as_bytes(),
                key(i).as_bytes(),
            )
            .unwrap();
        }
        db.compact_range(Some(b"a"), Some(b"z")).unwrap();
        for i in 0..n {
            db.put(
                WriteOptions::default(),
                key(i).as_bytes(),
                key(i).as_bytes(),
            )
            .unwrap();
        }
        db.inner.force_compact_mem_table().unwrap();
        store.delay_data_sync.store(true, Ordering::Release);
        for i in 0..n {
            let v = db.get(ReadOptions::default(), key(i).as_bytes()).unwrap();
            assert_eq!(v, Some(key(i).into_bytes()), "key {}", key(i));
        }
        let reads = store.random_read_counter.load(Ordering::Relaxed);
        assert!(reads >= n && reads <= n + 2 * n / 100);
        store.random_read_counter.store(0, Ordering::Relaxed);
        for i in 0..n {
            assert_eq!(
                None,
                db.get(ReadOptions::default(), (key(i) + ".missing").as_bytes())
                    .unwrap()
            )
        }
        let reads = store.random_read_counter.load(Ordering::Relaxed);
        assert!(reads <= 3 * n / 100);
    }

    const THREAD_COUNT: usize = 4;
    const TEST_SECONDS: usize = 10;
    const KEY_NUM: usize = 1000;

    impl DBTest {
        fn new_multi_thd_test(&self) -> MultiThreadTest {
            MultiThreadTest {
                db: self.db.clone(),
                // store: self.store.clone(),
                stop: Arc::new(AtomicBool::new(false)),
                // options: self.opt.clone(),
                states: Vec::with_capacity(THREAD_COUNT),
            }
        }
    }
    struct MultiThreadTest {
        stop: Arc<AtomicBool>,
        db: TemplateDB<MemStorage, BytewiseComparator>,
        states: Vec<Arc<ThreadState>>,
    }

    struct ThreadState {
        db: TemplateDB<MemStorage, BytewiseComparator>,
        stop: Arc<AtomicBool>,
        // The set-get runs
        counter: AtomicUsize,
        done: AtomicBool,
        werrs: Arc<Mutex<Vec<TemplateKVError>>>,
        rerrs: Arc<Mutex<Vec<TemplateKVError>>>,
    }

    unsafe impl Send for ThreadState {}
    unsafe impl Sync for ThreadState {}

    impl MultiThreadTest {
        fn start(&mut self, id: usize) {
            let state = Arc::new(ThreadState {
                db: self.db.clone(),
                stop: self.stop.clone(),
                counter: AtomicUsize::new(0),
                done: AtomicBool::new(false),
                werrs: Arc::new(Mutex::new(Vec::new())),
                rerrs: Arc::new(Mutex::new(Vec::new())),
            });
            self.states.push(state.clone());
            thread::Builder::new()
                .name(id.to_string())
                .spawn(move || {
                    println!("===== starting thread {}", id);
                    let mut counter = 0;
                    let mut rnd = rand::thread_rng();
                    while !state.stop.load(Ordering::Acquire) {
                        state.counter.store(counter, Ordering::Release);
                        let key = rand::thread_rng().gen_range(0, KEY_NUM);
                        if rnd.gen_range(1, 3) == 1 {
                            // Write values of the form <key, id, counter>
                            let value = format!("{}.{}.{}", key, id, counter);
                            match state.db.put(
                                WriteOptions::default(),
                                key.to_string().as_bytes(),
                                value.as_bytes(),
                            ) {
                                Ok(_) => continue,
                                Err(e) => {
                                    let mut guard = state.werrs.lock().unwrap();
                                    guard.push(e);
                                    break;
                                }
                            }
                        }
                        match state
                            .db
                            .get(ReadOptions::default(), key.to_string().as_bytes())
                        {
                            Ok(v) => {
                                if let Some(value) = v {
                                    let s = String::from_utf8(value).unwrap();
                                    let ss = s.split('.').collect::<Vec<_>>();
                                    assert_eq!(3, ss.len());
                                    assert_eq!(ss[0], key.to_string());
                                }
                            }
                            Err(e) => {
                                let mut guard = state.rerrs.lock().unwrap();
                                guard.push(e);
                                break;
                            }
                        }
                        counter += 1;
                    }
                    state.done.store(true, Ordering::Release);
                    println!(
                        "===== stopping thread {} after {} opts: write error {}, read error {}",
                        id,
                        counter,
                        state.werrs.lock().unwrap().len(),
                        state.rerrs.lock().unwrap().len()
                    );
                })
                .unwrap();
        }
    }

    #[test]
    fn test_multi_thread() {
        for t in default_cases() {
            let mut mt = t.new_multi_thd_test();
            for id in 0..THREAD_COUNT {
                mt.start(id);
            }
            thread::sleep(Duration::from_secs(TEST_SECONDS as u64));
            mt.stop.store(true, Ordering::Release);
            for state in mt.states.iter() {
                while !state.done.load(Ordering::Acquire) {
                    thread::sleep(Duration::from_millis(100));
                }
                {
                    let werrs = state.werrs.lock().unwrap();
                    assert_eq!(0, werrs.len(), "{:?}", werrs);
                    let rerrs = state.rerrs.lock().unwrap();
                    assert_eq!(0, rerrs.len(), "{:?}", rerrs);
                }
            }
        }
    }
}

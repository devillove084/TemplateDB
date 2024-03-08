use std::cmp::Ordering;

use self::key_format::InternalKeyComparator;
use crate::util::{comparator::Comparator, varint::VarintU32};

pub mod arena;
pub mod batch;
pub mod inlineskiplist;
pub mod key_format;
pub mod memtable;
pub mod skiplist;
pub mod value_format;

// use crate::{
//     db::format::{InternalKeyComparator, LookupKey, ValueType, INTERNAL_KEY_TAIL},
// error::{MemtableResult, TemplateResult, TemplateKVError}, iterator::Iterator, memtable::{
//         arena::OffsetArena,
//         inlineskiplist::{InlineSkipList, InlineSkiplistIterator},
//     }, util::{
//         coding::{decode_fixed_64, put_fixed_64},
//         comparator::Comparator,
//         varint::VarintU32,
//     }
// };

// #[async_trait::async_trait]
// pub trait MemtableTrait {
//     async fn insert(&self, key: impl Into<Bytes>, value: impl Into<Bytes>) -> MemtableResult<impl
// Into<Bytes>>;

//     async fn update(&self, key: impl Into<Bytes>, value: impl Into<Bytes>) -> MemtableResult<impl
// Into<Bytes>>;

//     async fn get(&self, key: impl Into<Bytes>) -> MemtableResult<impl Into<Bytes>>;

//     async fn scan(&self, range: Range<impl Index<usize>>) -> MemtableResult<Vec<(impl
// Into<Bytes>, impl Into<Bytes>)>>;

//     async fn delete(&self, key: impl Into<Bytes>) -> MemtableResult<bool>;
// }

// KeyComparator is a wrapper for InternalKeyComparator. It will convert the input mem key
// to the internal key before comparing.
#[derive(Clone, Default)]
pub struct KeyComparator<C: Comparator> {
    icmp: InternalKeyComparator<C>,
}

impl<C: Comparator> Comparator for KeyComparator<C> {
    // `a` and `b` should be a `LookupKey` each
    fn compare(&self, mut a: &[u8], mut b: &[u8]) -> Ordering {
        let ia = extract_varint32_encoded_slice(&mut a);
        let ib = extract_varint32_encoded_slice(&mut b);
        if ia.is_empty() || ib.is_empty() {
            // Use memcmp directly
            ia.cmp(ib)
        } else {
            self.icmp.compare(ia, ib)
        }
    }

    fn name(&self) -> &str {
        self.icmp.name()
    }

    fn separator(&self, mut a: &[u8], mut b: &[u8]) -> Vec<u8> {
        let ia = extract_varint32_encoded_slice(&mut a);
        let ib = extract_varint32_encoded_slice(&mut b);
        self.icmp.separator(ia, ib)
    }

    fn successor(&self, mut key: &[u8]) -> Vec<u8> {
        let ia = extract_varint32_encoded_slice(&mut key);
        self.icmp.successor(ia)
    }
}

// Decodes the length (varint u32) from `src` and advances it.
pub fn extract_varint32_encoded_slice<'a>(src: &mut &'a [u8]) -> &'a [u8] {
    if src.is_empty() {
        return src;
    }
    VarintU32::get_varint_prefixed_slice(src).unwrap_or(src)
}

#[cfg(test)]
mod tests {
    use std::str;

    use super::{key_format::InternalKeyComparator, memtable::MemTable, value_format::ValueType};
    use crate::{
        iterator::Iterator,
        memtable::key_format::{LookupKey, ParsedInternalKey},
        util::comparator::BytewiseComparator,
    };

    fn new_mem_table() -> MemTable<BytewiseComparator> {
        let icmp = InternalKeyComparator::new(BytewiseComparator::default());
        MemTable::new(1 << 32, icmp)
    }

    fn add_test_data_set(memtable: &MemTable<BytewiseComparator>) -> Vec<(&str, &str)> {
        let tests = vec![
            (2, ValueType::Value, "boo", "boo"),
            (4, ValueType::Value, "foo", "val3"),
            (3, ValueType::Deletion, "foo", ""),
            (2, ValueType::Value, "foo", "val2"),
            (1, ValueType::Value, "foo", "val1"),
        ];
        let mut results = vec![];
        for (seq, t, key, value) in tests.clone().drain(..) {
            memtable.add(seq, t, key.as_bytes(), value.as_bytes());
            results.push((key, value));
        }
        results
    }

    #[test]
    fn test_memtable_add_get() {
        let memtable = new_mem_table();
        memtable.add(1, ValueType::Value, b"foo", b"val1");
        memtable.add(2, ValueType::Value, b"foo", b"val2");
        memtable.add(3, ValueType::Deletion, b"foo", b"");
        memtable.add(4, ValueType::Value, b"foo", b"val3");
        memtable.add(2, ValueType::Value, b"boo", b"boo");

        let v = memtable.get(&LookupKey::new(b"null", 10));
        assert!(v.is_none());
        let v = memtable.get(&LookupKey::new(b"foo", 10));
        assert_eq!(b"val3", v.unwrap().unwrap().as_slice());
        let v = memtable.get(&LookupKey::new(b"foo", 0));
        assert!(v.is_none());
        let v = memtable.get(&LookupKey::new(b"foo", 1));
        assert_eq!(b"val1", v.unwrap().unwrap().as_slice());
        let v = memtable.get(&LookupKey::new(b"foo", 3));
        assert!(v.unwrap().is_err());
        let v = memtable.get(&LookupKey::new(b"boo", 3));
        assert_eq!(b"boo", v.unwrap().unwrap().as_slice());
    }

    #[test]
    fn test_memtable_iter() {
        let memtable = new_mem_table();
        let mut iter = memtable.iter();
        assert!(!iter.valid());
        let entries = add_test_data_set(&memtable);
        // Forward scan
        iter.seek_to_first();
        assert!(iter.valid());
        for (key, value) in entries.iter() {
            let k = iter.key();
            let pkey = ParsedInternalKey::decode_from(k).unwrap();
            assert_eq!(
                pkey.as_str(),
                *key,
                "expected key: {:?}, but got {:?}",
                *key,
                pkey.as_str()
            );
            assert_eq!(
                str::from_utf8(iter.value()).unwrap(),
                *value,
                "expected value: {:?}, but got {:?}",
                *value,
                str::from_utf8(iter.value()).unwrap()
            );
            iter.next();
        }
        assert!(!iter.valid());

        // Backward scan
        iter.seek_to_last();
        assert!(iter.valid());
        for (key, value) in entries.iter().rev() {
            let k = iter.key();
            let pkey = ParsedInternalKey::decode_from(k).unwrap();
            assert_eq!(
                pkey.as_str(),
                *key,
                "expected key: {:?}, but got {:?}",
                *key,
                pkey.as_str()
            );
            assert_eq!(
                str::from_utf8(iter.value()).unwrap(),
                *value,
                "expected value: {:?}, but got {:?}",
                *value,
                str::from_utf8(iter.value()).unwrap()
            );
            iter.prev();
        }
        assert!(!iter.valid());
    }
}

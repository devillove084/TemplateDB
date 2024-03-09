use std::cmp::Ordering;

use super::{
    arena::OffsetArena,
    extract_varint32_encoded_slice,
    inlineskiplist::{InlineSkipList, InlineSkiplistIterator},
    key_format::{InternalKeyComparator, LookupKey},
    value_format::ValueType,
    KeyComparator,
};
use crate::{
    error::{TemplateKVError, TemplateResult},
    iterator::{memtable_iter::MemTableIterator, Iterator},
    options::INTERNAL_KEY_TAIL,
    util::{
        coding::{decode_fixed_64, put_fixed_64},
        comparator::Comparator,
        varint::VarintU32,
    },
};

// In-memory write buffer
#[derive(Clone)]
pub struct MemTable<C: Comparator> {
    cmp: KeyComparator<C>,
    table: InlineSkipList<KeyComparator<C>, OffsetArena>,
}

impl<C: Comparator> MemTable<C> {
    /// Creates a new memory table
    pub fn new(max_mem_size: usize, icmp: InternalKeyComparator<C>) -> Self {
        let arena = OffsetArena::with_capacity(max_mem_size);
        let kcmp = KeyComparator { icmp };
        let table = InlineSkipList::new(kcmp.clone(), arena);
        Self { cmp: kcmp, table }
    }

    /// Returns an estimate of the number of bytes of data in use by this
    /// data structure. It is safe to call when MemTable is being modified.
    #[inline]
    pub fn approximate_memory_usage(&self) -> usize {
        self.table.total_size()
    }

    /// Creates a new `MemTableIterator`
    #[inline]
    pub fn iter(&self) -> MemTableIterator<C> {
        MemTableIterator::new(self.table.clone())
    }

    /// Returns current elements count in inner Skiplist
    #[inline]
    pub fn len(&self) -> usize {
        self.table.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.table.len() == 0
    }

    /// Add an entry into memtable that maps key to value at the
    /// specified sequence number and with the specified type.
    /// Typically value will be empty if the type is `Deletion`.
    ///
    /// The 'key' and 'value' will be bundled together into an 'entry':
    ///
    /// ```text
    ///   +=================================+
    ///   |       format of the entry       |
    ///   +=================================+
    ///   | varint32 of internal key length |
    ///   +---------------------------------+ ---------------
    ///   | user key bytes                  |
    ///   +---------------------------------+   internal key
    ///   | sequence (7)       |   type (1) |
    ///   +---------------------------------+ ---------------
    ///   | varint32 of value length        |
    ///   +---------------------------------+
    ///   | value bytes                     |
    ///   +---------------------------------+
    /// ```
    pub fn add(&self, seq_number: u64, val_type: ValueType, key: &[u8], value: &[u8]) {
        let key_size = key.len();
        let internal_key_size = key_size + INTERNAL_KEY_TAIL;
        let mut buf = vec![];
        VarintU32::put_varint(&mut buf, internal_key_size as u32);
        buf.extend_from_slice(key);
        put_fixed_64(
            &mut buf,
            (seq_number << INTERNAL_KEY_TAIL) | val_type as u64,
        );
        VarintU32::put_varint_prefixed_slice(&mut buf, value);
        self.table.put(buf);
    }

    /// If memtable contains a value for key, returns it in `Some(Ok())`.
    /// If memtable contains a deletion for key, returns `Some(Err(Status::NotFound))` .
    /// If memtable does not contain the key, return `None`
    pub fn get(&self, key: &LookupKey) -> Option<TemplateResult<Vec<u8>>> {
        let mk = key.mem_key();
        let mut iter = InlineSkiplistIterator::new(self.table.clone());
        iter.seek(mk);
        if iter.valid() {
            let mut e = iter.key();
            let ikey = extract_varint32_encoded_slice(&mut e);
            let key_size = ikey.len();
            // only check the user key here
            match self
                .cmp
                .icmp
                .user_comparator
                .compare(&ikey[..key_size - INTERNAL_KEY_TAIL], key.user_key())
            {
                Ordering::Equal => {
                    let tag = decode_fixed_64(&ikey[key_size - INTERNAL_KEY_TAIL..]);
                    match ValueType::from(tag & 0xff_u64) {
                        ValueType::Value => {
                            return Some(Ok(extract_varint32_encoded_slice(&mut e).to_vec()))
                        }
                        ValueType::Deletion => return Some(Err(TemplateKVError::NotFound(None))),
                        ValueType::Unknown => { /* fallback to None*/ }
                    }
                }
                _ => return None,
            }
        }
        None
    }
}

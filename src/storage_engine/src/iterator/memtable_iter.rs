use super::Iterator;
use crate::{
    error::TemplateResult,
    memtable::{
        arena::OffsetArena,
        extract_varint32_encoded_slice,
        inlineskiplist::{InlineSkipList, InlineSkiplistIterator},
        KeyComparator,
    },
    util::{comparator::Comparator, varint::VarintU32},
};

pub struct MemTableIterator<C: Comparator> {
    iter: InlineSkiplistIterator<KeyComparator<C>, OffsetArena>,
    // Tmp buffer for encoding `InternalKey` to `LookupKey` when call `seek`
    tmp: Vec<u8>,
}

impl<C: Comparator> MemTableIterator<C> {
    pub fn new(table: InlineSkipList<KeyComparator<C>, OffsetArena>) -> Self {
        let iter = InlineSkiplistIterator::new(table);
        Self { iter, tmp: vec![] }
    }
}

impl<C: Comparator> Iterator for MemTableIterator<C> {
    fn valid(&self) -> bool {
        self.iter.valid()
    }

    fn seek_to_first(&mut self) {
        self.iter.seek_to_first()
    }

    fn seek_to_last(&mut self) {
        self.iter.seek_to_last()
    }

    // target should be an encoded `LookupKey`
    fn seek(&mut self, target: &[u8]) {
        self.tmp.clear();
        VarintU32::put_varint_prefixed_slice(&mut self.tmp, target);
        self.iter.seek(&self.tmp)
    }

    fn next(&mut self) {
        self.iter.next()
    }

    fn prev(&mut self) {
        self.iter.prev()
    }

    // Returns the internal key
    fn key(&self) -> &[u8] {
        let mut key = self.iter.key();
        extract_varint32_encoded_slice(&mut key)
    }

    // Returns the Slice represents the value
    fn value(&self) -> &[u8] {
        let mut key = self.iter.key();
        extract_varint32_encoded_slice(&mut key);
        extract_varint32_encoded_slice(&mut key)
    }

    fn status(&mut self) -> TemplateResult<()> {
        Ok(())
    }
}

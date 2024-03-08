use std::sync::Arc;

use crate::{
    error::TemplateResult,
    manager::snapshot::Snapshot,
    memtable::batch::WriteBatch,
    options::{ReadOptions, WriteOptions},
};

/// A `DB` is a persistent ordered map from keys to values.
/// A `DB` is safe for concurrent access from multiple threads without
/// any external synchronization.
pub trait DB {
    /// The iterator that can yield all the kv pairs in `DB`
    type Iterator;

    /// `put` sets the value for the given key. It overwrites any previous value
    /// for that key; a DB is not a multi-map.
    fn put(&self, write_opt: WriteOptions, key: &[u8], value: &[u8]) -> TemplateResult<()>;

    /// `get` gets the value for the given key. It returns `None` if the DB
    /// does not contain the key.
    fn get(&self, read_opt: ReadOptions, key: &[u8]) -> TemplateResult<Option<Vec<u8>>>;

    /// Return an iterator over the contents of the database.
    fn iter(&self, read_opt: ReadOptions) -> TemplateResult<Self::Iterator>;

    /// `delete` deletes the value for the given key. It returns `Status::NotFound` if
    /// the DB does not contain the key.
    fn delete(&self, write_opt: WriteOptions, key: &[u8]) -> TemplateResult<()>;

    /// `write` applies the operations contained in the `WriteBatch` to the DB atomically.
    fn write(&self, write_opt: WriteOptions, batch: WriteBatch) -> TemplateResult<()>;

    /// `close` shuts down the current TemplateDB by waiting util all the background tasks are
    /// complete and then releases the file lock. A closed db should never be used again and is
    /// able to be dropped safely.
    fn close(&mut self) -> TemplateResult<()>;

    /// `destroy` shuts down the current TemplateDB and delete all relative files and the db
    /// directory.
    fn destroy(&mut self) -> TemplateResult<()>;

    /// Acquire a `Snapshot` for reading DB
    fn snapshot(&self) -> Arc<Snapshot>;
}

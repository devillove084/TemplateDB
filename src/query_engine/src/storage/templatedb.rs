use std::sync::{Arc, Mutex};

use storage_engine::{
    db_impl::template_impl::TemplateDB, storage::mem::MemStorage,
    util::comparator::BytewiseComparator,
};

use crate::catalog::RootCatalog;

use super::{InMemoryTable, Storage};

#[allow(dead_code)]
pub struct TemplateDBIndependent {
    db: Arc<TemplateDB<MemStorage, BytewiseComparator>>,
    catalog: Mutex<RootCatalog>,
}

impl TemplateDBIndependent {
    pub fn new(db: Arc<TemplateDB<MemStorage, BytewiseComparator>>) -> Self {
        Self {
            db,
            catalog: Mutex::new(RootCatalog::new()),
        }
    }
}

impl Storage for TemplateDBIndependent {
    type TableType = InMemoryTable;

    fn create_csv_table(&self, _id: String, _filepath: String) -> Result<(), super::StorageError> {
        unimplemented!()
    }

    fn create_mem_table(
        &self,
        _id: String,
        _data: Vec<arrow::array::RecordBatch>,
    ) -> Result<(), super::StorageError> {
        todo!()
    }

    fn get_table(&self, _id: String) -> Result<Self::TableType, super::StorageError> {
        todo!()
    }

    fn get_catalog(&self) -> crate::catalog::RootCatalog {
        self.catalog.lock().unwrap().clone()
    }

    fn show_tables(&self) -> Result<arrow::array::RecordBatch, super::StorageError> {
        todo!()
    }
}

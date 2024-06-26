use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::hash::Hash;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field};

use super::entry::{CatalogEntry, DataTable};
use super::{
    CatalogError, CatalogSet, ScalarFunctionCatalogEntry, TableCatalogEntry,
    TableFunctionCatalogEntry,
};
use crate::common::{CreateScalarFunctionInfo, CreateTableFunctionInfo};
use crate::main_entry::ClientContext;

pub type RootCatalogRef = Arc<RootCatalog>;

#[derive(Debug, Clone)]
pub struct RootCatalog {
    pub tables: HashMap<TableId, TableCatalog>,
}

impl Default for RootCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl RootCatalog {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    pub fn get_table_by_name(&self, name: &str) -> Option<TableCatalog> {
        self.tables.get(name).cloned()
    }
}

/// use table name as id for simplicity
pub type TableId = String;

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct TableCatalog {
    pub id: TableId,
    pub name: String,
    /// column_ids to keep the order of inferred columns
    pub column_ids: Vec<ColumnId>,
    pub columns: BTreeMap<ColumnId, ColumnCatalog>,
}

impl TableCatalog {
    pub fn get_column_by_name(&self, name: &str) -> Option<ColumnCatalog> {
        self.columns.get(name).cloned()
    }

    pub fn get_all_columns(&self) -> Vec<ColumnCatalog> {
        self.column_ids
            .iter()
            .map(|id| self.columns.get(id).cloned().unwrap())
            .collect()
    }

    pub fn new_from_columns(table_id: String, columns: Vec<ColumnCatalog>) -> Self {
        let mut columns_tree = BTreeMap::new();
        let mut column_ids = Vec::new();
        for c in columns {
            column_ids.push(c.column_id.clone());
            columns_tree.insert(c.column_id.clone(), c);
        }
        TableCatalog {
            id: table_id.to_string(),
            name: table_id,
            column_ids,
            columns: columns_tree,
        }
    }

    /// Only change column catalog table id to alias, keep original id
    pub fn clone_with_new_column_table_id(&self, table_id: String) -> Self {
        let mut columns_tree = BTreeMap::new();
        for c in self.get_all_columns() {
            columns_tree.insert(c.column_id.clone(), c.clone_with_table_id(table_id.clone()));
        }
        TableCatalog {
            id: self.id.clone(),
            name: self.name.clone(),
            column_ids: self.column_ids.clone(),
            columns: columns_tree,
        }
    }
}

/// use column name as id for simplicity
pub type ColumnId = String;

#[derive(Clone)]
pub struct ColumnCatalog {
    pub table_id: TableId,
    pub column_id: ColumnId,
    pub nullable: bool,
    pub desc: ColumnDesc,
}

impl ColumnCatalog {
    pub fn new(
        table_id: TableId,
        column_id: ColumnId,
        nullable: bool,
        data_type: DataType,
    ) -> Self {
        Self {
            table_id,
            column_id: column_id.clone(),
            nullable,
            desc: ColumnDesc {
                name: column_id,
                data_type,
            },
        }
    }

    pub fn clone_with_table_id(&self, table_id: TableId) -> Self {
        Self {
            table_id,
            column_id: self.column_id.clone(),
            nullable: self.nullable,
            desc: self.desc.clone(),
        }
    }

    pub fn clone_with_nullable(&self, nullable: bool) -> ColumnCatalog {
        let mut c = self.clone();
        c.nullable = nullable;
        c
    }

    pub fn to_arrow_field(&self) -> Field {
        Field::new(
            format!("{}.{}", self.table_id, self.column_id).as_str(),
            self.desc.data_type.clone(),
            self.nullable,
        )
    }
}

/// Only compare table_id and column_id, so it's safe to compare join output cols with nullable col.
impl PartialEq for ColumnCatalog {
    fn eq(&self, other: &Self) -> bool {
        self.table_id == other.table_id && self.column_id == other.column_id
    }
}

impl Eq for ColumnCatalog {}

impl Hash for ColumnCatalog {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.table_id.hash(state);
        self.column_id.hash(state);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnDesc {
    pub name: String,
    pub data_type: DataType,
}

impl fmt::Debug for ColumnCatalog {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let type_str = if self.nullable {
            format!("Nullable({:?})", self.desc.data_type)
        } else {
            self.desc.data_type.to_string()
        };
        write!(f, "{}.{}:{}", self.table_id, self.column_id, type_str)
    }
}

impl fmt::Debug for TableCatalog {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            r#"{} {{ columns: {:?} }}"#,
            self.id,
            self.get_all_columns(),
        )
    }
}

/// The Catalog object represents the catalog of the database.
#[derive(Clone, Debug, Default)]
pub struct Catalog {
    /// The catalog set holding the schemas
    schemas: CatalogSet,
    /// The catalog version, incremented whenever anything changes in the catalog
    catalog_version: usize,
}

impl Catalog {
    pub fn create_schema(&mut self, name: String) -> Result<(), CatalogError> {
        self.catalog_version += 1;
        let entry = CatalogEntry::default_schema_catalog_entry(self.catalog_version, name.clone());
        self.schemas.create_entry(name, entry)
    }

    pub fn create_table(
        client_context: Arc<ClientContext>,
        schema: String,
        table: String,
        data_table: DataTable,
    ) -> Result<(), CatalogError> {
        let mut catalog = match client_context.db.catalog.try_write() {
            Ok(c) => c,
            Err(_) => return Err(CatalogError::CatalogLockedError),
        };
        if let CatalogEntry::SchemaCatalogEntry(mut entry) =
            catalog.schemas.get_entry(schema.clone())?
        {
            catalog.catalog_version += 1;
            entry.create_table(catalog.catalog_version, table, data_table)?;
            catalog
                .schemas
                .replace_entry(schema, CatalogEntry::SchemaCatalogEntry(entry))?;
            return Ok(());
        }
        Err(CatalogError::CatalogEntryTypeNotMatch)
    }

    pub fn get_table(
        client_context: Arc<ClientContext>,
        schema: String,
        table: String,
    ) -> Result<TableCatalogEntry, CatalogError> {
        let catalog = match client_context.db.catalog.try_read() {
            Ok(c) => c,
            Err(_) => return Err(CatalogError::CatalogLockedError),
        };
        if let CatalogEntry::SchemaCatalogEntry(entry) = catalog.schemas.get_entry(schema)? {
            return entry.get_table(table);
        }
        Err(CatalogError::CatalogEntryTypeNotMatch)
    }

    pub fn create_table_function(
        client_context: Arc<ClientContext>,
        info: CreateTableFunctionInfo,
    ) -> Result<(), CatalogError> {
        let mut catalog = match client_context.db.catalog.try_write() {
            Ok(c) => c,
            Err(_) => return Err(CatalogError::CatalogLockedError),
        };
        if let CatalogEntry::SchemaCatalogEntry(mut entry) =
            catalog.schemas.get_entry(info.base.schema.clone())?
        {
            catalog.catalog_version += 1;
            let schema = info.base.schema.clone();
            entry.create_table_function(catalog.catalog_version, info)?;
            catalog
                .schemas
                .replace_entry(schema, CatalogEntry::SchemaCatalogEntry(entry))?;
            return Ok(());
        }
        Err(CatalogError::CatalogEntryTypeNotMatch)
    }

    pub fn scan_entries<F>(
        client_context: Arc<ClientContext>,
        schema: String,
        callback: &F,
    ) -> Result<Vec<CatalogEntry>, CatalogError>
    where
        F: Fn(&CatalogEntry) -> bool,
    {
        let catalog = match client_context.db.catalog.try_read() {
            Ok(c) => c,
            Err(_) => return Err(CatalogError::CatalogLockedError),
        };
        if let CatalogEntry::SchemaCatalogEntry(entry) = catalog.schemas.get_entry(schema)? {
            return Ok(entry.scan_entries(callback));
        }
        Err(CatalogError::CatalogEntryTypeNotMatch)
    }

    pub fn get_table_function(
        client_context: Arc<ClientContext>,
        schema: String,
        table_function: String,
    ) -> Result<TableFunctionCatalogEntry, CatalogError> {
        let catalog = match client_context.db.catalog.try_read() {
            Ok(c) => c,
            Err(_) => return Err(CatalogError::CatalogLockedError),
        };
        if let CatalogEntry::SchemaCatalogEntry(entry) = catalog.schemas.get_entry(schema)? {
            return entry.get_table_function(table_function);
        }
        Err(CatalogError::CatalogEntryTypeNotMatch)
    }

    pub fn create_scalar_function(
        client_context: Arc<ClientContext>,
        info: CreateScalarFunctionInfo,
    ) -> Result<(), CatalogError> {
        let mut catalog = match client_context.db.catalog.try_write() {
            Ok(c) => c,
            Err(_) => return Err(CatalogError::CatalogLockedError),
        };
        let version = catalog.catalog_version;
        let entry = catalog.schemas.get_mut_entry(info.base.schema.clone())?;

        if let CatalogEntry::SchemaCatalogEntry(mut_entry) = entry {
            mut_entry.create_scalar_function(version + 1, info)?;
            catalog.catalog_version += 1;
            Ok(())
        } else {
            Err(CatalogError::CatalogEntryTypeNotMatch)
        }
    }

    pub fn get_scalar_function(
        client_context: Arc<ClientContext>,
        schema: String,
        scalar_function: String,
    ) -> Result<ScalarFunctionCatalogEntry, CatalogError> {
        let catalog = match client_context.db.catalog.try_read() {
            Ok(c) => c,
            Err(_) => return Err(CatalogError::CatalogLockedError),
        };
        if let CatalogEntry::SchemaCatalogEntry(entry) = catalog.schemas.get_entry(schema)? {
            return entry.get_scalar_function(scalar_function);
        }
        Err(CatalogError::CatalogEntryTypeNotMatch)
    }
}

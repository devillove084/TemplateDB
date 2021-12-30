use std::{collections::HashMap, io::ErrorKind};

use tokio::io::AsyncRead;

use crate::{codec, codec::Value, Result};

pub struct TableReader {
    map: HashMap<Vec<u8>, Value>,
}

impl TableReader {
    pub async fn new<R: AsyncRead + Unpin>(mut r: R) -> Result<TableReader> {
        let map = read_all(&mut r).await?;
        Ok(TableReader { map })
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Value>> {
        Ok(self.map.get(key).cloned())
    }
}

type IoResult<T> = std::result::Result<T, std::io::Error>;

async fn read_all<R: AsyncRead + Unpin>(r: &mut R) -> IoResult<HashMap<Vec<u8>, Value>> {
    let mut map = HashMap::new();
    loop {
        match codec::read_record(r).await {
            Ok(record) => {
                assert!(map.insert(record.0, record.1).is_none());
            }
            Err(err) => {
                if err.kind() == ErrorKind::UnexpectedEof {
                    return Ok(map);
                } else {
                    return Err(err);
                }
            }
        }
    }
}

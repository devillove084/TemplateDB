use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::{codec, codec::Value, Result};

pub struct TableBuilder<W: AsyncWrite + Unpin> {
    write: W,
    block: BlockBuilder,
}

impl<W: AsyncWrite + Unpin> TableBuilder<W> {
    pub fn new(write: W) -> Self {
        Self {
            write,
            block: BlockBuilder::new(),
        }
    }

    pub async fn add(&mut self, key: &[u8], value: &Value) -> Result<()> {
        self.block.add(key, value);
        if self.block.size() >= BLOCK_SIZE {
            self.flush().await?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        self.write.write_all(self.block.data()).await?;
        self.block.reset();
        Ok(())
    }

    pub async fn finish(mut self) -> Result<()> {
        if self.block.size() > 0 {
            self.flush().await?;
        }
        self.write.shutdown().await?;
        Ok(())
    }
}

const BLOCK_SIZE: usize = 8 * 1024;

struct BlockBuilder {
    buf: Vec<u8>,
}

impl BlockBuilder {
    fn new() -> Self {
        Self {
            buf: Vec::with_capacity(BLOCK_SIZE),
        }
    }

    fn add(&mut self, key: &[u8], value: &Value) {
        codec::put_record(&mut self.buf, key, value);
    }

    fn data(&self) -> &[u8] {
        &self.buf
    }

    fn size(&self) -> usize {
        self.buf.len()
    }

    fn reset(&mut self) {
        self.buf.clear()
    }
}

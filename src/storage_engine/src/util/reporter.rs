use std::{cell::RefCell, rc::Rc};

use crate::{
    error::{TemplateKVError, TemplateResult},
    wal::wal_record_reader::Reporter,
};

#[derive(Clone)]
pub struct LogReporter {
    inner: Rc<RefCell<LogReporterInner>>,
}

struct LogReporterInner {
    ok: bool,
    reason: String,
}

impl LogReporter {
    pub fn new() -> Self {
        Self {
            inner: Rc::new(RefCell::new(LogReporterInner {
                ok: true,
                reason: "".to_owned(),
            })),
        }
    }
    pub fn result(&self) -> TemplateResult<()> {
        let inner = self.inner.borrow();
        if inner.ok {
            Ok(())
        } else {
            Err(TemplateKVError::Corruption(inner.reason.clone()))
        }
    }
}

impl Reporter for LogReporter {
    fn corruption(&mut self, _bytes: u64, reason: &str) {
        self.inner.borrow_mut().ok = false;
        self.inner.borrow_mut().reason = reason.to_owned();
    }
}

impl Default for LogReporter {
    fn default() -> Self {
        Self::new()
    }
}

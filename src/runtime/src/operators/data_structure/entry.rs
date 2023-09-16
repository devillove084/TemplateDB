pub struct EntryOperator {
    entry: Box<dyn OpeartionOn>,
}

#[async_trait::async_trait]
pub trait OpeartionOn {}

use crossbeam_channel::RecvError;
use quick_error::quick_error;

quick_error! {
    #[derive(Debug)]
    pub enum TemplateKVError {
        /// If the hint is `None`, the key is deleted
        NotFound(hint: Option<String>) {
            display("key seeking failed: {:?}", hint)
        }
        Corruption(hint: String) {
            display("data corruption: {}", hint)
        }
        UTF8Error(err: std::string::FromUtf8Error) {
            display("UTF8 error: {:?}", err)
        }
        InvalidArgument(hint: String) {
            display("invalid argument: {}", hint)
        }
        DBClosed(hint: String) {
            display("try to operate a closed db: {}", hint)
        }
        CompressionFailed(err: snap::Error) {
            display("compression failed: {}", err)
            cause(err)
        }
        IO(err: std::io::Error) {
            display("I/O operation error: {}", err)
            cause(err)
        }
        RecvError(err: RecvError) {
            display("{:?}", err)
            cause(err)
        }
        Customized(hint: String) {
            display("{}", hint)
        }
    }
}

macro_rules! map_io_res {
    ($result:expr) => {
        match $result {
            Ok(v) => Ok(v),
            Err(e) => Err(TemplateKVError::IO(e)),
        }
    };
}

pub type TemplateResult<T> = std::result::Result<T, TemplateKVError>;

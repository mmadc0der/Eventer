use std::fmt;

/// Recoverable store failure. Variants carry owned text so they can cross threads.
#[derive(Debug, Clone)]
pub enum Error {
    Io(String),
    Json(String),
    Schema(String),
    Event(String),
    Corrupt(String),
    Closed,
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub fn io(err: impl fmt::Display) -> Self {
        Error::Io(err.to_string())
    }

    pub fn json(err: impl fmt::Display) -> Self {
        Error::Json(err.to_string())
    }

    pub fn schema(err: impl fmt::Display) -> Self {
        Error::Schema(err.to_string())
    }

    pub fn event(err: impl fmt::Display) -> Self {
        Error::Event(err.to_string())
    }

    pub fn corrupt(err: impl fmt::Display) -> Self {
        Error::Corrupt(err.to_string())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(msg) => write!(f, "io error: {msg}"),
            Error::Json(msg) => write!(f, "json error: {msg}"),
            Error::Schema(msg) => write!(f, "schema error: {msg}"),
            Error::Event(msg) => write!(f, "event error: {msg}"),
            Error::Corrupt(msg) => write!(f, "corrupt data: {msg}"),
            Error::Closed => write!(f, "store is closed"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Error::io(value)
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Error::json(value)
    }
}

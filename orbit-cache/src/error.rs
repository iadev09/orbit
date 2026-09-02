use std::fmt;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub enum Error {
    KeyTooLarge { key_len: usize, max: usize },
    ValueTooLarge { value_len: usize, max: usize },
    InvalidLayout(&'static str),
    Io(std::io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::KeyTooLarge { key_len, max } => {
                write!(f, "cache key is too large: {key_len} > {max}")
            }
            Self::ValueTooLarge { value_len, max } => {
                write!(f, "cache value is too large: {value_len} > {max}")
            }
            Self::InvalidLayout(message) => write!(f, "invalid cache layout: {message}"),
            Self::Io(error) => write!(f, "orbit cache io error: {error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

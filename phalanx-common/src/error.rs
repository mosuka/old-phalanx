use std::fmt::{self, Display};
use std::io::Error as IOError;

use failure::{Backtrace, Context, Fail};

#[derive(Debug, Fail)]
pub enum ErrorKind {
    #[fail(display = "CLI error: {}", 0)]
    Cli(String),
    #[fail(display = "IO error")]
    Io,
    #[fail(display = "Not found")]
    NotFound(String),
    #[fail(display = "Parse error")]
    ParseError(String),
    #[fail(display = "Other error")]
    Other(String),
}

#[derive(Debug)]
pub struct Error {
    inner: Context<ErrorKind>,
}

impl Fail for Error {
    fn cause(&self) -> Option<&dyn Fail> {
        self.inner.cause()
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        self.inner.backtrace()
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Display::fmt(&self.inner, f)
    }
}

impl Error {
    pub fn new(inner: Context<ErrorKind>) -> Error {
        Error { inner }
    }

    pub fn kind(&self) -> &ErrorKind {
        self.inner.get_context()
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Error {
        Error {
            inner: Context::new(kind),
        }
    }
}

impl From<Context<ErrorKind>> for Error {
    fn from(inner: Context<ErrorKind>) -> Error {
        Error { inner }
    }
}

impl From<IOError> for Error {
    fn from(error: IOError) -> Error {
        Error {
            inner: error.context(ErrorKind::Io),
        }
    }
}

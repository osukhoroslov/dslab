use std::fmt::{Error, Formatter};

use serde::Serialize;

#[derive(Serialize, Clone)]
pub struct Message {
    pub tip: String,
    pub data: String,
}

impl Message {
    pub fn new<T>(tip: T, data: T) -> Self
    where
        T: Into<String>,
    {
        Self {
            tip: tip.into(),
            data: data.into(),
        }
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }
}

impl std::fmt::Debug for Message {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "{} {}", self.tip, self.data)
    }
}

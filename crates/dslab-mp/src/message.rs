use std::fmt::{Error, Formatter};

use serde::Serialize;

#[derive(Serialize, Clone, Eq, Hash, PartialEq, PartialOrd, Ord)]
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

    pub fn json<T, S>(tip: T, data: &S) -> Self
    where
        T: Into<String>,
        S: Serialize,
    {
        Self {
            tip: tip.into(),
            data: serde_json::to_string_pretty(data)
                .unwrap()
                .replace('\n', "")
                .replace("  ", ""),
        }
    }

    pub fn size(&self) -> usize {
        self.tip.len() + self.data.len()
    }
}

impl std::fmt::Debug for Message {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "{} {}", self.tip, self.data)
    }
}

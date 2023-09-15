//! Message definition.

use std::fmt::{Error, Formatter};

use serde::Serialize;

/// Represents a message.
#[derive(Serialize, Clone, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct Message {
    /// Message type.
    pub tip: String,
    /// Message data (payload).
    pub data: String,
}

impl Message {
    /// Creates a message.
    pub fn new<T>(tip: T, data: T) -> Self
    where
        T: Into<String>,
    {
        Self {
            tip: tip.into(),
            data: data.into(),
        }
    }

    /// Creates a message with JSON serialized payload.
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

    /// Returns the message size as the sum of message type and data lengths.
    pub fn size(&self) -> usize {
        self.tip.len() + self.data.len()
    }
}

impl std::fmt::Debug for Message {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "{} {}", self.tip, self.data)
    }
}

use crate::mc::events::McEvent;

#[derive(Debug, Default, PartialEq)]
pub struct McError {
    str: String,
    trace: Vec<McEvent>,
}

impl McError {
    pub fn new(str: String, trace: Vec<McEvent>) -> Self {
        Self { str, trace }
    }
}

impl McError {
    pub fn str(&self) -> String {
        self.str.clone()
    }

    pub fn trace(&self) -> Vec<McEvent> {
        self.trace.clone()
    }
}

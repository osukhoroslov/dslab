use serde::Serialize;

#[derive(Serialize, Clone, Debug)]
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
}

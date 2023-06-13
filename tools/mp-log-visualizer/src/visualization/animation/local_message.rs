#[derive(Debug, Clone)]
pub struct StateLocalMessage {
    pub id: String,
    pub time: f64,
    pub node: String,
    pub tip: String,
    pub data: String,
    pub msg_type: LocalMessageType,
}

impl StateLocalMessage {
    pub fn new(id: String, time: f64, node: String, tip: String, data: String, msg_type: LocalMessageType) -> Self {
        Self {
            id,
            time,
            node,
            tip,
            data,
            msg_type,
        }
    }
}

#[derive(Clone, Debug)]
pub enum LocalMessageType {
    Sent,
    Received,
}

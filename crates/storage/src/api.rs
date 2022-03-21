use serde::Serialize;

#[derive(Serialize)]
pub struct DataReadRequest {
    pub size: u64,
}

#[derive(Serialize)]
pub struct DataWriteRequest {
    pub size: u64,
}

#[derive(Serialize)]
pub struct DataReadCompleted {
    pub src_event_id: u64,
    pub size: u64,
}

#[derive(Serialize)]
pub struct DataWriteCompleted {
    pub src_event_id: u64,
    pub size: u64,
}

#[derive(Serialize)]
pub struct FileReadRequest {
    pub file_name: String,
    pub size: Option<u64>,
}

#[derive(Serialize)]
pub struct FileWriteRequest {
    pub file_name: String,
    pub size: u64,
}

#[derive(Serialize)]
pub struct FileReadCompleted {
    pub file_name: String,
    pub read_size: u64,
}

#[derive(Serialize)]
pub struct FileWriteCompleted {
    pub file_name: String,
    pub new_size: u64,
}

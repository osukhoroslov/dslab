use serde::Serialize;

#[derive(Serialize)]
pub struct DataReadCompleted {
    pub request_id: u64,
    pub size: u64,
}

#[derive(Serialize)]
pub struct DataWriteCompleted {
    pub request_id: u64,
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

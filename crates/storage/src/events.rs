use serde::Serialize;

// Disk events

#[derive(Serialize, Clone)]
pub struct DataReadCompleted {
    pub request_id: u64,
    pub size: u64,
}

#[derive(Serialize, Clone)]
pub struct DataReadFailed {
    pub request_id: u64,
    pub error: String,
}

#[derive(Serialize, Clone)]
pub struct DataWriteCompleted {
    pub request_id: u64,
    pub size: u64,
}

#[derive(Serialize, Clone)]
pub struct DataWriteFailed {
    pub request_id: u64,
    pub error: String,
}

// File events

#[derive(Serialize, Clone)]
pub struct FileReadCompleted {
    pub request_id: u64,
    pub file_name: String,
    pub read_size: u64,
}

#[derive(Serialize, Clone)]
pub struct FileReadFailed {
    pub request_id: u64,
    pub file_name: String,
    pub error: String,
}

#[derive(Serialize, Clone)]
pub struct FileWriteCompleted {
    pub request_id: u64,
    pub file_name: String,
    pub new_size: u64,
}

#[derive(Serialize, Clone)]
pub struct FileWriteFailed {
    pub request_id: u64,
    pub file_name: String,
    pub error: String,
}

#[derive(Debug)]
pub struct DataReadRequest {
    pub size: u64,
}

#[derive(Debug)]
pub struct DataWriteRequest {
    pub size: u64,
}

#[derive(Debug)]
pub struct DataReadCompleted {
    pub src_event_id: u64,
    pub size: u64,
}

#[derive(Debug)]
pub struct DataWriteCompleted {
    pub src_event_id: u64,
    pub size: u64,
}

#[derive(Debug)]
pub struct FileReadRequest {
    pub file_name: String,
    pub size: Option<u64>,
}

#[derive(Debug)]
pub struct FileWriteRequest {
    pub file_name: String,
    pub size: u64,
}

#[derive(Debug)]
pub struct FileReadCompleted {
    pub file_name: String,
    pub read_size: u64,
}

#[derive(Debug)]
pub struct FileWriteCompleted {
    pub file_name: String,
    pub new_size: u64,
}

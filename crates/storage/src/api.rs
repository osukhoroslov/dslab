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
    pub size: u64
}

#[derive(Debug)]
pub struct DataWriteCompleted {
    pub src_event_id: u64,
    pub size: u64
}


pub type FD = u64;

#[derive(Debug)]
pub struct FileReadRequest {
    pub fd: FD,
    pub size: u64,
}

#[derive(Debug)]
pub struct FileWriteRequest {
    pub fd: FD,
    pub size: u64,
}

#[derive(Debug)]
pub struct FileReadCompleted {
    pub fd: FD,
}

#[derive(Debug)]
pub struct FileWriteCompleted {
    pub fd: FD,
    pub new_size: u64,
}

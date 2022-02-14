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
}

#[derive(Debug)]
pub struct DataWriteCompleted {
    pub src_event_id: u64,
}

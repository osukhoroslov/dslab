#[derive(Debug)]
pub struct IOReadRequest {
    pub start: u64,
    pub count: u64
}

#[derive(Debug)]
pub struct IOWriteRequest {
    pub start: u64,
    pub count: u64
}

#[derive(Debug)]
pub struct IOReadCompleted {
    pub src_event_id: u64
}

#[derive(Debug)]
pub struct IOWriteCompleted {
    pub src_event_id: u64
}

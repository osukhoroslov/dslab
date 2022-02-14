#[derive(Debug, Clone, Copy)]
pub struct TaskRequest {
    pub id: u64,
    pub flops: u64,
    pub cpus: u32,
    pub memory: u64,
    pub input_size: u64,
    pub output_size: u64,
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum TaskState {
    New,
    Assigned,
    Downloading,
    Reading,
    Running,
    Writing,
    Uploading,
    Completed,
    Failed,
    Canceled,
}

#[derive(Debug)]
pub struct TaskInfo {
    pub(crate) req: TaskRequest,
    pub(crate) state: TaskState,
}

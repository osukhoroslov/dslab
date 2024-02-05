use serde::Serialize;

use dslab_compute::multicore::CoresDependency;

#[derive(Serialize, Debug, Clone)]
pub struct JobRequest {
    pub id: u64,
    pub flops: f64,
    pub memory: u64,
    pub min_cores: u32,
    pub max_cores: u32,
    pub cores_dependency: CoresDependency,
    pub input_size: u64,
    pub output_size: u64,
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum JobState {
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
pub struct JobInfo {
    pub(crate) req: JobRequest,
    pub(crate) state: JobState,
}

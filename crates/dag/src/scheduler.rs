pub struct Resource {
    pub speed: u64,
    pub cores_available: u32,
    pub memory_available: u64,
}

pub enum Action {
    Schedule { task: usize, resource: usize, cores: u32 },
}

pub trait Scheduler {
    fn start(&mut self) -> Vec<Action>;
    fn on_task_completed(&mut self, task: usize) -> Vec<Action>;
}

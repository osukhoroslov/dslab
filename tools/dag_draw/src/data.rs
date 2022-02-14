use druid::Color;

#[derive(Debug, PartialEq, Clone)]
pub struct TaskInfo {
    pub scheduled: f64,
    pub started: f64,
    pub completed: f64,
    pub cores: u32,
    pub id: usize,
    pub color: Color,
}

#[derive(Debug)]
pub struct File {
    pub start: f64,
    pub uploaded: f64,
    pub end: f64,
    pub name: String,
}

#[derive(Debug)]
pub struct Transfer {
    pub start: f64,
    pub end: f64,
    pub from: String,
    pub to: String,
    pub name: String,
    pub task: usize,
}

#[derive(Debug)]
pub struct Compute {
    pub name: String,
    pub speed: u64,
    pub cores: u32,
    pub memory: u64,
    pub files: Vec<File>,
    pub tasks: Vec<usize>,
}

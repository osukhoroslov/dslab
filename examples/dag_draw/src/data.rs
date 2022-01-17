use druid::Color;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize)]
pub struct TraceLog {
    pub resources: Vec<Value>,
    pub events: Vec<Value>,
}

#[derive(Debug)]
pub struct Task {
    pub scheduled: f64,
    pub started: f64,
    pub completed: f64,
    pub name: String,
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
    #[allow(dead_code)]
    pub to: String,
    pub name: String,
}

#[derive(Debug)]
pub struct Compute {
    pub name: String,
    pub speed: u64,
    pub cores: u64,
    pub files: Vec<File>,
    pub tasks: Vec<Task>,
}

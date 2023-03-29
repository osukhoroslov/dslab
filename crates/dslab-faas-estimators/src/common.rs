#[derive(Clone, Default)]
pub struct Instance {
    pub hosts: Vec<Vec<u64>>,
    pub apps: Vec<Vec<u64>>,
    pub app_coldstart: Vec<u64>,
    pub req_app: Vec<usize>,
    pub req_dur: Vec<u64>,
    pub req_start: Vec<u64>,
    pub keepalive: u64,
}

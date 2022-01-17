#[derive(Debug)]
pub struct BlockDevice {
    pub name: String,
    pub throughput: f64, // of sequential scan
    pub latency: f64, // of random read
    pub volume: u64,
    pub current_block_id: u64
}

impl BlockDevice {
    pub fn new(name: String, throughput: f64, latency: f64, volume: u64) -> Self {
        Self {name, throughput, latency, volume, current_block_id: 0}
    }
}

#[derive(Debug, Clone)]
pub struct Computation {
    pub flops: u64,
    pub memory: u64,
}

impl Computation {
    pub fn new(flops: u64, memory: u64) -> Self {
        Self { flops, memory }
    }
}

#[derive(Default)]
pub struct Counter {
    value: u64,
}

impl Counter {
    pub fn curr(&self) -> u64 {
        self.value
    }

    pub fn increment(&mut self) -> u64 {
        let curr = self.value;
        self.value += 1;
        curr
    }
}

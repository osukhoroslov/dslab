#[derive(Default)]
pub struct Counter {
    body: u64,
}

impl Counter {
    pub fn curr(&self) -> u64 {
        self.body
    }

    pub fn next(&mut self) -> u64 {
        let id = self.body;
        self.body += 1;
        id
    }
}

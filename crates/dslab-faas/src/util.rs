#[derive(Default)]
pub struct Counter {
    value: u64,
}

impl Counter {
    pub fn curr(&self) -> u64 {
        self.value
    }

    pub fn next(&mut self) -> u64 {
        let curr = self.value;
        self.value += 1;
        curr
    }
}

#[derive(Default, Copy, Clone)]
pub struct KahanSum {
    sum: f64,
    c: f64,
}

impl KahanSum {
    pub fn new(start: f64) -> Self {
        Self { sum: start, c: 0. }
    }

    pub fn add(&mut self, x: f64) {
        let y = x - self.c;
        let t = self.sum + y;
        self.c = (t - self.sum) - y;
        self.sum = t;
    }

    pub fn get(&self) -> f64 {
        self.sum
    }

    pub fn reset(&mut self) {
        self.sum = 0.;
        self.c = 0.;
    }
}

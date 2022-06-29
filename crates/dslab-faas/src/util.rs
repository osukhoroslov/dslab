use std::ops::{AddAssign, Deref, SubAssign};

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
}

impl Deref for KahanSum {
    type Target = f64;

    fn deref(&self) -> &Self::Target {
        &self.sum
    }
}

impl AddAssign<f64> for KahanSum {
    fn add_assign(&mut self, other: f64) {
        let y = other - self.c;
        let t = self.sum + y;
        self.c = (t - self.sum) - y;
        self.sum = t;
    }
}

impl SubAssign<f64> for KahanSum {
    fn sub_assign(&mut self, other: f64) {
        *self += -other;
    }
}

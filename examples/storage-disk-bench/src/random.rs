const A: u64 = 737687;
const B: u64 = 65916437;
const MOD: u64 = 1000000007;

pub struct CustomRandom {
    state: u64,
}

impl CustomRandom {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    pub fn next(&mut self) -> u64 {
        self.state = (A * self.state + B) % MOD;
        self.state
    }
}

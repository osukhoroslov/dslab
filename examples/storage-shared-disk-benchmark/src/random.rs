const A: u64 = 737687;
const B: u64 = 65916437;
const MOD: u64 = 1000000007;

pub struct CustomRandom {
    state: u64,
}

impl CustomRandom {
    pub fn new(seed: u64) -> Self {
        println!("Created linear random engine with parameters:");
        println!("A     = {}", A);
        println!("B     = {}", B);
        println!("MOD   = {}", MOD);
        println!("SEED  = {}", seed);

        Self { state: seed }
    }

    pub fn next(&mut self) -> u64 {
        self.state = (A * self.state + B) % MOD;
        self.state
    }
}

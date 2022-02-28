static VM_START_DURATION: f64 = 1.0;
static VM_STOP_DURATION: f64 = 0.5;

#[derive(Debug)]
pub struct VirtualMachine {
    lifetime: f64,
}

impl VirtualMachine {
    pub fn new(lifetime: f64) -> Self {
        Self { lifetime }
    }

    pub fn lifetime(&self) -> f64 {
        self.lifetime
    }

    pub fn start_duration(&self) -> f64 {
        VM_START_DURATION
    }

    pub fn stop_duration(&self) -> f64 {
        VM_STOP_DURATION
    }
}

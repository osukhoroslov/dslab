use crate::container::Container;

pub trait KeepalivePolicy {
    fn keepalive_period(&mut self, container: &Container) -> f64;
}

pub struct FixedKeepalivePolicy {
    period: f64,
}

impl FixedKeepalivePolicy {
    pub fn new(period: f64) -> Self {
        Self { period }
    }
}

impl KeepalivePolicy for FixedKeepalivePolicy {
    fn keepalive_period(&mut self, _container: &Container) -> f64 {
        self.period
    }
}

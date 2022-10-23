pub trait AnnealingSchedule {
    fn get_temperature(&mut self) -> f64;
    /// resets the schedule before the next local search run
    fn reset(&mut self);
}

pub struct ExponentialAnnealingSchedule {
    t: f64,
    t_init: f64,
    coeff: f64,
}

impl ExponentialAnnealingSchedule {
    pub fn new(t: f64, coeff: f64) -> Self {
        Self { t, t_init: t, coeff }
    }
}

impl AnnealingSchedule for ExponentialAnnealingSchedule {
    fn get_temperature(&mut self) -> f64 {
        let t = self.t;
        self.t *= self.coeff;
        t
    }

    fn reset(&mut self) {
        self.t = self.t_init;
    }
}

pub trait LoadModel {
    fn get_resource_load(&self, time: f64, time_from_start: f64) -> f64;
}

pub struct DefaultLoadModel;

impl DefaultLoadModel {
    pub fn new() -> Self {
        Self {}
    }
}

impl LoadModel for DefaultLoadModel {
    fn get_resource_load(&self, _time: f64, _time_from_start: f64) -> f64 {
        1.
    }
}

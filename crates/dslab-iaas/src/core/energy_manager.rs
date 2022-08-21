//! Energy manager, which calculates host energy consumption.

/// Energy manager structure.
#[derive(Debug, Clone)]
pub struct EnergyManager {
    energy_consumed: f64,
    prev_milestone: f64,
    current_load: f64,
}

impl EnergyManager {
    /// Creates component.
    pub fn new() -> Self {
        Self {
            prev_milestone: 0.0,
            energy_consumed: 0.0,
            current_load: 0.0,
        }
    }

    /// Invoked each time CPU load is changed to update the total energy consumption.
    pub fn update_energy(&mut self, time: f64, new_load: f64) {
        self.energy_consumed += (time - self.prev_milestone) * self.current_load;
        self.current_load = new_load;
        self.prev_milestone = time;
    }

    /// Returns the total energy consumption.
    pub fn get_total_consumed(&self) -> f64 {
        return self.energy_consumed;
    }
}

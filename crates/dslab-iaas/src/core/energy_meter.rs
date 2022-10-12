//! Energy meter calculates the host energy consumption.

/// Energy meter structure.
#[derive(Debug, Clone)]
pub struct EnergyMeter {
    energy_consumed: f64,
    current_power: f64,
    prev_time: f64,
}

impl EnergyMeter {
    /// Creates component.
    pub fn new() -> Self {
        Self {
            prev_time: 0.0,
            current_power: 0.0,
            energy_consumed: 0.0,
        }
    }

    /// Invoked each time the host power consumption is changed to update the total energy consumption.
    pub fn update(&mut self, time: f64, power: f64) {
        self.energy_consumed += (time - self.prev_time) * self.current_power;
        self.current_power = power;
        self.prev_time = time;
    }

    /// Returns the total energy consumption.
    pub fn energy_consumed(&self) -> f64 {
        return self.energy_consumed;
    }
}

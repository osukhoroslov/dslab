//! Memory power consumption model provided by Micron.

use crate::power::memory::MemoryPowerModel;

/// Micron power model for DDR3 sticks.
///
/// (c) https://www.micron.com/-/media/client/global/documents/products/technical-note/dram/tn41_01ddr3_power.pdf
/// Using this paper we assume, that typical memory stick power consumption consists of:
/// 1) Some static power consumption (background power, 113.5 mW in paper) - 26% of maximum consumption.
/// 2) Activate consumption for every read and write actions (activate power, 123.2 mW in paper) - 28%
/// 3) Read and write activities (RD/WR/Term power, 199.3 mW in paper) - 46%.
/// In addition, we split the third part into to pieces - wrire (89.1 mW or 20%) and read (110.1 mW or 26%).
///
/// Then we assume that typical 8GB DDR3 stick consumes about 3W of power.
/// (c) buildcomputers.net https://www.buildcomputers.net/power-consumption-of-pc-components.html
///
/// Total power consumption then relies on memory amount, e.g. host with 240 GB of RAM has 30 of these sticks.
/// Thus it consumes about 90W of power at maximut footprint.
#[derive(Clone)]
pub struct MicronPowerModel {
    static_percentage: f64,
    activate_percentage: f64,
    read_percentage: f64,
    write_percentage: f64,

    stick_size: f64,
    total_consumption: f64,
    memory_size: f64,
}

impl MicronPowerModel {
    /// Creates Micron power model.
    pub fn new(memory_size: f64) -> Self {
        Self {
            static_percentage: 0.26,
            activate_percentage: 0.28,
            read_percentage: 0.26,
            write_percentage: 0.2,
            stick_size: 8.,
            total_consumption: 3.,
            memory_size,
        }
    }

    /// Creates Micron power model with user specific memory stick parameters.
    ///
    /// * `stick_size` - Memory stick size in GB.
    /// * `power_consumption` - Memory stick power consumption at maximum footprint.
    pub fn custom_model(memory_size: f64, stick_size: f64, power_consumption: f64) -> Self {
        Self {
            static_percentage: 0.26,
            activate_percentage: 0.28,
            read_percentage: 0.26,
            write_percentage: 0.2,
            stick_size,
            total_consumption: power_consumption,
            memory_size,
        }
    }

    /// Return memory sticks count in current host.
    pub fn sticks_count(&self) -> u32 {
        ((self.memory_size + self.stick_size - 1.) / self.stick_size) as u32
    }
}

impl MemoryPowerModel for MicronPowerModel {
    fn get_power(&self, utilization: f64) -> f64 {
        let dynamic = (self.activate_percentage + self.read_percentage + self.write_percentage) * utilization;
        (self.static_percentage + dynamic) * self.total_consumption * (self.sticks_count() as f64)
    }

    fn get_power_adv(&self, read_util: f64, write_util: f64) -> f64 {
        let activate_util = (read_util + write_util) / 2.;
        let dynamic = self.activate_percentage * activate_util
            + self.read_percentage * read_util
            + self.write_percentage * write_util;
        (self.static_percentage + dynamic) * self.total_consumption * (self.sticks_count() as f64)
    }
}

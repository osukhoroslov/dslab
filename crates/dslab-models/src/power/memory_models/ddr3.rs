//! DDR3 SDRAM power model.

use crate::power::memory::MemoryPowerModel;

/// A power model for DDR3 SDRAM based on the technical note from Micron:
/// https://www.micron.com/-/media/client/global/documents/products/technical-note/dram/tn41_01ddr3_power.pdf
///
/// Based on this document we assume that the power consumption of a typical DDR3 module consists of:
/// 1) Static power consumption (background power, 113.5 mW in the document) - 26% of maximum consumption
/// 2) Activate consumption for every read and write action (activate power, 123.2 mW) - 28%
/// 3) Read and write activities (RD/WR/Term power, 199.3 mW in paper) - 46%
/// In addition, we split the third part into two pieces - write (89.1 mW or 20%) and read (110.1 mW or 26%).
///
/// Then we assume that a typical 8GB DDR3 module consumes 3 W of power:
/// https://www.buildcomputers.net/power-consumption-of-pc-components.html
/// https://www.crucial.com/support/articles-faq-memory/how-much-power-does-memory-use
///
/// The total power consumption then relies on memory amount, e.g. a host with 240 GB of RAM has 30 such modules.
/// Thus it consumes 90 W of power at maximum utilization.
#[derive(Clone)]
pub struct Ddr3MemoryPowerModel {
    static_percentage: f64,
    activate_percentage: f64,
    read_percentage: f64,
    write_percentage: f64,
    module_size: f64,
    module_max_consumption: f64,
    memory_size: f64,
}

impl Ddr3MemoryPowerModel {
    /// Creates DDR3 power model.
    pub fn new(memory_size: f64) -> Self {
        Self {
            static_percentage: 0.26,
            activate_percentage: 0.28,
            read_percentage: 0.26,
            write_percentage: 0.2,
            module_size: 8.,
            module_max_consumption: 3.,
            memory_size,
        }
    }

    /// Creates DDR3 power model with custom memory module parameters.
    ///
    /// * `module_size` - Memory module size in GB.
    /// * `module_max_consumption` - Memory module power consumption at maximum utilization.
    pub fn custom_model(memory_size: f64, module_size: f64, module_max_consumption: f64) -> Self {
        Self {
            static_percentage: 0.26,
            activate_percentage: 0.28,
            read_percentage: 0.26,
            write_percentage: 0.2,
            module_size,
            module_max_consumption,
            memory_size,
        }
    }

    /// Returns the memory modules count.
    pub fn modules_count(&self) -> u32 {
        ((self.memory_size + self.module_size - 1.) / self.module_size) as u32
    }
}

impl MemoryPowerModel for Ddr3MemoryPowerModel {
    fn get_power_simple(&self, utilization: f64) -> f64 {
        let dynamic = (self.activate_percentage + self.read_percentage + self.write_percentage) * utilization;
        (self.static_percentage + dynamic) * self.module_max_consumption * (self.modules_count() as f64)
    }

    fn get_power_advanced(&self, read_util: f64, write_util: f64) -> f64 {
        let activate_util = (read_util + write_util) / 2.;
        let dynamic = self.activate_percentage * activate_util
            + self.read_percentage * read_util
            + self.write_percentage * write_util;
        (self.static_percentage + dynamic) * self.module_max_consumption * (self.modules_count() as f64)
    }
}

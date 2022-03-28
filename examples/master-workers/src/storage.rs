use serde::Serialize;

use core::context::SimulationContext;

#[derive(Serialize)]
pub struct DataReadCompleted {
    pub id: u64,
}

#[derive(Serialize)]
pub struct DataWriteCompleted {
    pub id: u64,
}

pub struct Storage {
    read_bandwidth: u64,
    write_bandwidth: u64,
    ready_time: f64,
    next_id: u64,
    sim: SimulationContext,
}

impl Storage {
    pub fn new(read_bandwidth: u64, write_bandwidth: u64, sim: SimulationContext) -> Self {
        Self {
            read_bandwidth,
            write_bandwidth,
            ready_time: 0.,
            next_id: 0,
            sim,
        }
    }

    pub fn read(&mut self, size: u64, requester: u32) -> u64 {
        let req_id = self.next_id;
        self.next_id += 1;
        let read_time = size as f64 / self.read_bandwidth as f64;
        self.ready_time = self.ready_time.max(self.sim.time()) + read_time;
        let delay = self.ready_time - self.sim.time();
        self.sim.emit(DataReadCompleted { id: req_id }, requester, delay);
        req_id
    }

    pub fn write(&mut self, size: u64, requester: u32) -> u64 {
        let req_id = self.next_id;
        self.next_id += 1;
        let write_time = size as f64 / self.write_bandwidth as f64;
        self.ready_time = self.ready_time.max(self.sim.time()) + write_time;
        let delay = self.ready_time - self.sim.time();
        self.sim.emit(DataWriteCompleted { id: req_id }, requester, delay);
        req_id
    }
}

use serde::Serialize;
use simcore::cast;
use simcore::event::Event;
use simcore::handler::EventHandler;

use simcore::component::Id;
use simcore::{context::SimulationContext, log_debug, log_error};

use crate::events::{DataReadCompleted, DataReadFailed, DataWriteCompleted, DataWriteFailed};
use crate::fair_sharing::*;

#[derive(Serialize, Clone)]
struct DiskReadTask {
    requester: Id,
    on_complete_event: DataReadCompleted,
}

#[derive(Serialize, Clone)]
struct DiskWriteTask {
    requester: Id,
    on_complete_event: DataWriteCompleted,
}

pub struct SharedDisk {
    capacity: u64,
    used: u64,
    read_throughput_model: FairThroughputSharingModel<DiskReadTask>,
    write_throughput_model: FairThroughputSharingModel<DiskWriteTask>,
    next_request_id: u64,
    ctx: SimulationContext,
    next_read_event: u64,
    next_write_event: u64,
}

impl SharedDisk {
    pub fn new(capacity: u64, read_bandwidth: u64, write_bandwidth: u64, ctx: SimulationContext) -> Self {
        Self {
            capacity,
            used: 0,
            read_throughput_model: FairThroughputSharingModel::new(read_bandwidth as f64),
            write_throughput_model: FairThroughputSharingModel::new(write_bandwidth as f64),
            next_request_id: 0,
            ctx,
            next_read_event: 0,
            next_write_event: 0,
        }
    }

    fn get_unique_request_id(&mut self) -> u64 {
        let request_id = self.next_request_id;
        self.next_request_id += 1;
        request_id
    }

    pub fn read(&mut self, size: u64, requester: Id) -> u64 {
        log_debug!(
            self.ctx,
            "Received read request, size: {}, requester: {}",
            size,
            requester
        );
        let request_id = self.get_unique_request_id();
        if size > self.capacity {
            let error = format!(
                "requested read size is {} but only {} is available",
                size, self.capacity
            );
            log_error!(self.ctx, "Failed reading: {}", error,);
            self.ctx.emit_now(DataReadFailed { request_id, error }, requester);
        } else {
            self.read_throughput_model.insert(
                self.ctx.time(),
                size as f64,
                DiskReadTask {
                    requester,
                    on_complete_event: DataReadCompleted { request_id, size },
                },
            );
            self.reschedule_read_top_event();
        }
        request_id
    }

    pub fn write(&mut self, size: u64, requester: Id) -> u64 {
        let request_id = self.get_unique_request_id();
        log_debug!(
            self.ctx,
            "Received write request, size: {}, requester: {}",
            size,
            requester
        );
        let available = self.capacity - self.used;
        if available < size {
            let error = format!("requested write size is {} but only {} is available", size, available);
            log_error!(self.ctx, "Failed writing: {}", error,);
            self.ctx.emit_now(DataWriteFailed { request_id, error }, requester);
        } else {
            self.used += size;
            self.write_throughput_model.insert(
                self.ctx.time(),
                size as f64,
                DiskWriteTask {
                    requester,
                    on_complete_event: DataWriteCompleted { request_id, size },
                },
            );
            self.reschedule_write_top_event();
        }
        request_id
    }

    fn reschedule_read_top_event(&mut self) {
        self.ctx.cancel_event(self.next_read_event);
        if let Some((time, event)) = self.read_throughput_model.peek() {
            self.next_read_event = self.ctx.emit_self(event.clone(), time - self.ctx.time());
        }
    }

    fn reschedule_write_top_event(&mut self) {
        self.ctx.cancel_event(self.next_write_event);
        if let Some((time, event)) = self.read_throughput_model.peek() {
            self.next_write_event = self.ctx.emit_self(event.clone(), time - self.ctx.time());
        }
    }

    pub fn mark_free(&mut self, size: u64) -> Result<(), String> {
        if size <= self.used {
            self.used -= size;
            return Ok(());
        }
        Err(format!("invalid size: {}", size))
    }

    pub fn get_used_space(&self) -> u64 {
        self.used
    }

    pub fn id(&self) -> Id {
        self.ctx.id()
    }
}

impl EventHandler for SharedDisk {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            DiskReadTask {
                requester,
                on_complete_event,
            } => {
                self.ctx.emit_now(on_complete_event, requester);
                self.read_throughput_model.pop().unwrap();
                self.reschedule_read_top_event();
            }
            DiskWriteTask {
                requester,
                on_complete_event,
            } => {
                self.ctx.emit_now(on_complete_event, requester);
                self.write_throughput_model.pop().unwrap();
                self.reschedule_write_top_event();
            }
        })
    }
}

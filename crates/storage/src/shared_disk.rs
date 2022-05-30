use serde::Serialize;

use simcore::cast;
use simcore::component::{Fractional, Id};
use simcore::event::Event;
use simcore::handler::EventHandler;
use simcore::{context::SimulationContext, log_debug, log_error};

use crate::events::{DataReadCompleted, DataReadFailed, DataWriteCompleted, DataWriteFailed};
use throughput_model::fair_sharing::FairThroughputSharingModel;
use throughput_model::model::ThroughputModel;

#[derive(Clone)]
struct DiskActivity {
    request_id: u64,
    requester: Id,
    size: u64,
}

#[derive(Serialize)]
pub struct DiskReadActivityCompleted {}

#[derive(Serialize)]
pub struct DiskWriteActivityCompleted {}

pub struct SharedDisk {
    capacity: u64,
    used: u64,
    read_throughput_model: FairThroughputSharingModel<DiskActivity>,
    write_throughput_model: FairThroughputSharingModel<DiskActivity>,
    next_request_id: u64,
    next_read_event: u64,
    next_write_event: u64,
    ctx: SimulationContext,
}

impl SharedDisk {
    pub fn new(capacity: u64, read_bandwidth: Fractional, write_bandwidth: Fractional, ctx: SimulationContext) -> Self {
        Self {
            capacity,
            used: 0,
            read_throughput_model: FairThroughputSharingModel::with_fixed_throughput(read_bandwidth),
            write_throughput_model: FairThroughputSharingModel::with_fixed_throughput(write_bandwidth),
            next_request_id: 0,
            next_read_event: 0,
            next_write_event: 0,
            ctx,
        }
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
                Fractional::from_integer(size.try_into().unwrap()),
                DiskActivity {
                    request_id,
                    requester,
                    size,
                },
            );
            self.ctx.cancel_event(self.next_read_event);
            self.schedule_next_read_event();
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
                Fractional::from_integer(size.try_into().unwrap()),
                DiskActivity {
                    request_id,
                    requester,
                    size,
                },
            );
            self.ctx.cancel_event(self.next_write_event);
            self.schedule_next_write_event();
        }
        request_id
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

    fn get_unique_request_id(&mut self) -> u64 {
        let request_id = self.next_request_id;
        self.next_request_id += 1;
        request_id
    }

    fn schedule_next_read_event(&mut self) {
        if let Some((time, _)) = self.read_throughput_model.next_time() {
            self.next_read_event = self.ctx.emit_self(DiskReadActivityCompleted {}, time - self.ctx.time());
        }
    }

    fn schedule_next_write_event(&mut self) {
        if let Some((time, _)) = self.read_throughput_model.next_time() {
            self.next_write_event = self
                .ctx
                .emit_self(DiskWriteActivityCompleted {}, time - self.ctx.time());
        }
    }

    fn on_read_completed(&mut self) {
        let (time, activity) = self.read_throughput_model.pop().unwrap();
        assert_eq!(time, self.ctx.time());
        self.ctx.emit_now(
            DataReadCompleted {
                request_id: activity.request_id,
                size: activity.size,
            },
            activity.requester,
        );
        self.schedule_next_read_event();
    }

    fn on_write_completed(&mut self) {
        let (time, activity) = self.write_throughput_model.pop().unwrap();
        assert_eq!(time, self.ctx.time());
        self.ctx.emit_now(
            DataWriteCompleted {
                request_id: activity.request_id,
                size: activity.size,
            },
            activity.requester,
        );
        self.schedule_next_write_event();
    }
}

impl EventHandler for SharedDisk {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            DiskReadActivityCompleted {} => {
                self.on_read_completed();
            }
            DiskWriteActivityCompleted {} => {
                self.on_write_completed();
            }
        })
    }
}

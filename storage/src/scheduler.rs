use crate::api::{IOReadCompleted, IOReadRequest, IOWriteCompleted, IOWriteRequest};
use crate::block_device::BlockDevice;

use core::actor::{Actor, ActorContext, ActorId, Event};
use core::cast;

use std::cell::RefCell;
use std::mem::swap;
use std::rc::Rc;

pub trait IOScheduler: Actor {}

#[derive(Debug)]
pub struct NoOpIOScheduler {
    pub device: Rc<RefCell<BlockDevice>>,
    release_time: f64,
}

impl IOScheduler for NoOpIOScheduler {}

impl NoOpIOScheduler {
    pub fn new(device: Rc<RefCell<BlockDevice>>) -> Self {
        Self {
            device,
            release_time: 0.0,
        }
    }

    fn calc_delay(&self, start: u64, count: u64) -> f64 {
        // TODO(kuskarov): find more elegant way
        let diff;
        if start > self.device.borrow().current_block_id {
            diff = start - self.device.borrow().current_block_id;
        } else {
            diff = self.device.borrow().current_block_id - start;
        }

        3.14 * diff as f64 + self.device.borrow().latency + count as f64 / self.device.borrow().throughput
    }
}

impl Actor for NoOpIOScheduler {
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            &IOReadRequest { start, count } => {
                let delay = self.calc_delay(start, count);

                if self.release_time < ctx.time() {
                    self.release_time = ctx.time()
                }
                self.release_time += delay;

                ctx.emit(
                    IOReadCompleted {
                        src_event_id: ctx.event_id,
                    },
                    from,
                    delay,
                );
            }
            &IOWriteRequest { start, count } => {
                let delay = self.calc_delay(start, count);

                if self.release_time < ctx.time() {
                    self.release_time = ctx.time()
                }
                self.release_time += delay;

                ctx.emit(
                    IOWriteCompleted {
                        src_event_id: ctx.event_id,
                    },
                    from,
                    delay,
                );
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}

#[derive(Debug)]
enum IORequestType {
    READ,
    WRITE,
}

#[derive(Debug)]
struct IORequest {
    req_type: IORequestType,
    start: u64,
    count: u64,
    from: ActorId,
    src_event_id: u64,
}

#[derive(Debug)]
pub struct TimerEvent {}

#[derive(Debug)]
pub struct ScanIOScheduler {
    id: ActorId,
    device: Rc<RefCell<BlockDevice>>,
    queueing_period: f64,
    pending_queue: Vec<IORequest>,
    processing_queue: Vec<IORequest>,
    is_waiting: bool,
}

impl IOScheduler for ScanIOScheduler {}

impl ScanIOScheduler {
    pub fn new(id: ActorId, device: Rc<RefCell<BlockDevice>>, queueing_period: f64) -> Self {
        Self {
            id,
            device,
            queueing_period,
            pending_queue: Vec::new(),
            processing_queue: Vec::new(),
            is_waiting: true,
        }
    }

    fn calc_delay(&self, start: u64, count: u64) -> f64 {
        // TODO(kuskarov): find more elegant way
        let diff;
        if start > self.device.borrow().current_block_id {
            diff = start - self.device.borrow().current_block_id;
        } else {
            diff = self.device.borrow().current_block_id - start;
        }

        3.14 * diff as f64 + self.device.borrow().latency + count as f64 / self.device.borrow().throughput
    }
}

impl Actor for ScanIOScheduler {
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            TimerEvent {} => {
                if self.processing_queue.len() == 0 {
                    swap(&mut self.processing_queue, &mut self.pending_queue);
                    self.processing_queue.sort_by_key(|req| u64::MAX - req.start);
                }
                if self.processing_queue.len() > 0 {
                    let req = self.processing_queue.pop().unwrap();
                    let delay = self.calc_delay(req.start, req.count);

                    // continue processing after delay
                    ctx.emit(TimerEvent {}, self.id.clone(), delay);

                    // notify caller that request is completed
                    match req.req_type {
                        IORequestType::READ => {
                            ctx.emit(
                                IOReadCompleted {
                                    src_event_id: req.src_event_id,
                                },
                                req.from,
                                delay,
                            );
                        }
                        IORequestType::WRITE => {
                            ctx.emit(
                                IOWriteCompleted {
                                    src_event_id: req.src_event_id,
                                },
                                req.from,
                                delay,
                            );
                        }
                    }
                }
            }
            &IOReadRequest { start, count } => {
                self.pending_queue.push(IORequest {
                    req_type: IORequestType::READ,
                    start,
                    count,
                    from: from.clone(),
                    src_event_id: ctx.event_id,
                });

                if self.is_waiting {
                    ctx.emit(TimerEvent {}, self.id.clone(), self.queueing_period);
                    self.is_waiting = false;
                }
            }
            &IOWriteRequest { start, count } => {
                self.pending_queue.push(IORequest {
                    req_type: IORequestType::WRITE,
                    start,
                    count,
                    from: from.clone(),
                    src_event_id: ctx.event_id,
                });

                if self.is_waiting {
                    ctx.emit(TimerEvent {}, self.id.clone(), self.queueing_period);
                    self.is_waiting = false;
                }
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}

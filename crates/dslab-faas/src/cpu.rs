/// This file contains the CPU sharing model used in the simulator.
/// We use something similar to CPUShares in cgroups, but instead of shares we operate with (shares/core_shares),
/// i. e. if the container has 512 shares and each core amounts to 1024 shares, we say that the share of the container equals 0.5.
/// If the container allows concurrent invocations, each invocation gets an equal part of the container share.
/// We assume that CPU sharing is fair: each invocation makes progress according to its share.
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};
use std::rc::Rc;

use dslab_core::context::SimulationContext;
use dslab_core::event::EventId;

use crate::container::Container;
use crate::event::InvocationEndEvent;
use crate::invocation::Invocation;

#[derive(Clone)]
pub struct WorkItem {
    finish: f64,
    id: u64,
}

impl PartialEq for WorkItem {
    fn eq(&self, other: &Self) -> bool {
        self.finish == other.finish && self.id == other.id
    }
}

impl Eq for WorkItem {}

impl PartialOrd for WorkItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for WorkItem {
    fn cmp(&self, other: &Self) -> Ordering {
        self.finish.total_cmp(&other.finish).then(self.id.cmp(&other.id))
    }
}

/// ProgressComputer computes invocation progress and manages invocation end events.
pub struct ProgressComputer {
    disable_contention: bool,
    work_tree: BTreeSet<WorkItem>,
    work_map: HashMap<u64, WorkItem>,
    work_total: f64,
    cores: f64,
    load: f64,
    last_update: f64,
    end_event: Option<EventId>,
    ctx: Rc<RefCell<SimulationContext>>,
}

impl ProgressComputer {
    pub fn get_load(&self) -> f64 {
        self.load
    }

    fn try_rebuild(&mut self) {
        if self.work_total > 1e12 {
            let mut items: Vec<_> = self.work_tree.iter().cloned().collect();
            for it in items.iter_mut() {
                it.finish -= self.work_total;
            }
            self.work_total = 0.;
            self.work_tree = Default::default();
            self.work_map = Default::default();
            for it in items.into_iter() {
                self.work_map.insert(it.id, it.clone());
                self.work_tree.insert(it);
            }
        }
    }

    fn reschedule_end(&mut self) {
        if let Some(evt) = self.end_event {
            self.ctx.borrow_mut().cancel_event(evt);
        }
        if !self.work_tree.is_empty() {
            let it = self.work_tree.iter().next().unwrap().clone();
            let delta = it.finish - self.work_total;
            self.end_event = Some(self.ctx.borrow_mut().emit_self(
                InvocationEndEvent { id: it.id },
                delta * f64::max(self.cores, self.load),
            ));
        } else {
            self.end_event = None;
        }
    }

    fn remove_invocation(&mut self, id: u64) -> f64 {
        let it = self.work_map.remove(&id).unwrap();
        self.work_tree.remove(&it);
        it.finish - self.work_total
    }

    fn insert_invocation(&mut self, id: u64, remain: f64) {
        let it = WorkItem {
            finish: self.work_total + remain,
            id,
        };
        self.work_map.insert(id, it.clone());
        self.work_tree.insert(it);
    }

    fn shift_time(&mut self, time: f64) {
        self.work_total += (time - self.last_update) / f64::max(self.cores, self.load);
        self.try_rebuild();
    }

    fn on_new_invocation(&mut self, invocation: &mut Invocation, container: &mut Container, time: f64) {
        if self.disable_contention {
            self.ctx
                .borrow_mut()
                .emit_self(InvocationEndEvent { id: invocation.id }, invocation.request.duration);
            return;
        }
        self.shift_time(time);
        if container.invocations.len() > 1 {
            let cnt = container.invocations.len() as f64;
            for i in container.invocations.iter().copied() {
                if i != invocation.id {
                    let remain = self.remove_invocation(i);
                    self.insert_invocation(i, remain * cnt / (cnt - 1.0));
                }
            }
        } else {
            self.load += container.cpu_share;
        }
        self.insert_invocation(
            invocation.id,
            invocation.request.duration / self.cores * (container.invocations.len() as f64),
        );
        self.last_update = time;
        self.reschedule_end();
    }

    fn on_invocation_end(&mut self, invocation: &mut Invocation, container: &mut Container, time: f64) {
        if self.disable_contention {
            return;
        }
        self.end_event = None;
        self.shift_time(time);
        self.remove_invocation(invocation.id);
        if !container.invocations.is_empty() {
            let cnt = container.invocations.len() as f64;
            for i in container.invocations.iter().copied() {
                let remain = self.remove_invocation(i);
                self.insert_invocation(i, remain * cnt / (cnt + 1.0));
            }
        } else {
            self.load -= container.cpu_share;
        }
        self.last_update = time;
        self.reschedule_end();
    }
}

pub struct CPU {
    pub cores: u32,
    progress_computer: ProgressComputer,
}

impl CPU {
    pub fn new(cores: u32, disable_contention: bool, ctx: Rc<RefCell<SimulationContext>>) -> Self {
        let progress_computer = ProgressComputer {
            disable_contention,
            work_tree: Default::default(),
            work_map: Default::default(),
            work_total: Default::default(),
            cores: cores as f64,
            load: Default::default(),
            last_update: 0.,
            end_event: None,
            ctx,
        };
        Self {
            cores,
            progress_computer,
        }
    }

    pub fn get_load(&self) -> f64 {
        self.progress_computer.get_load()
    }

    pub fn on_new_invocation(&mut self, invocation: &mut Invocation, container: &mut Container, time: f64) {
        self.progress_computer.on_new_invocation(invocation, container, time)
    }

    pub fn on_invocation_end(&mut self, invocation: &mut Invocation, container: &mut Container, time: f64) {
        self.progress_computer.on_invocation_end(invocation, container, time)
    }
}

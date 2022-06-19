/// A brief description of CPU sharing model used in this simulator.
/// We use something similar to CPUShares in cgroups, but instead of shares we operate with (shares/core_shares),
/// i. e. if the container has 512 shares and each core amounts to 1024 shares, we say that the share of the container equals 0.5.
/// If the container allows concurrent invocations, each invocation gets an equal part of the container share.
/// We assume that CPU sharing is fair: each invocation makes progress according to its share.
use std::boxed::Box;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};
use std::rc::Rc;

use simcore::context::SimulationContext;
use simcore::event::EventId;

use crate::container::{Container, ContainerManager};
use crate::event::InvocationEndEvent;
use crate::invocation::Invocation;
use crate::util::KahanSum;

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
        self.finish
            .partial_cmp(&other.finish)
            .unwrap()
            .then(self.id.cmp(&other.id))
    }
}

/// ProgressComputer computes invocation progress and manages invocation end events.
pub struct ProgressComputer {
    work_tree: BTreeSet<WorkItem>,
    work_map: HashMap<u64, WorkItem>,
    work_total: KahanSum,
    cores: f64,
    ctx: Rc<RefCell<SimulationContext>>,
    load: KahanSum,
    last_update: f64,
    end_event: Option<EventId>,
}

impl ProgressComputer {
    fn reschedule_end(&mut self) {
        if let Some(evt) = self.end_event {
            self.ctx.borrow_mut().cancel_event(evt);
        }
        if !self.work_tree.is_empty() {
            let it = self.work_tree.iter().next().unwrap().clone();
            let delta = it.finish - self.work_total.get();
            self.end_event = Some(self.ctx.borrow_mut().emit_self(
                InvocationEndEvent { id: it.id },
                delta * f64::max(self.cores, self.load.get()),
            ));
        } else {
            self.end_event = None;
        }
    }

    fn remove_invocation(&mut self, id: u64) -> f64 {
        let it = self.work_map.remove(&id).unwrap();
        self.work_tree.remove(&it);
        let delta = it.finish - self.work_total.get();
        delta
    }

    fn insert_invocation(&mut self, id: u64, remain: f64) {
        let it = WorkItem {
            finish: self.work_total.get() + remain,
            id,
        };
        self.work_map.insert(id, it.clone());
        self.work_tree.insert(it);
    }

    fn transform_time(&self, time: f64, share: f64, forward: bool) -> f64 {
        if forward {
            share * time / self.cores
        } else {
            time * self.cores / share
        }
    }

    fn shift_time(&mut self, time: f64) {
        self.work_total
            .add((time - self.last_update) / f64::max(self.cores, self.load.get()));
    }

    pub fn on_new_invocation(&mut self, invocation: &mut Invocation, container: &mut Container, time: f64) {
        self.shift_time(time);
        if container.invocations.len() > 1 {
            let cnt = container.invocations.len() as f64;
            for i in container.invocations.iter().copied() {
                if i != invocation.id {
                    let remain = self.remove_invocation(i);
                    self.insert_invocation(i, remain * cnt / (cnt - 1.0))
                }
            }
        } else {
            self.load.add(container.cpu_share);
        }
        self.insert_invocation(
            invocation.id,
            self.transform_time(
                invocation.request.duration,
                container.cpu_share / (container.invocations.len() as f64),
                true,
            ),
        );
        self.last_update = time;
        self.reschedule_end();
    }

    pub fn on_invocation_end(&mut self, invocation: &mut Invocation, container: &mut Container, time: f64) {
        self.end_event = None;
        self.shift_time(time);
        self.remove_invocation(invocation.id);
        if container.invocations.len() > 0 {
            let cnt = container.invocations.len() as f64;
            for i in container.invocations.iter().copied() {
                let remain = self.remove_invocation(i);
                self.insert_invocation(i, remain * cnt / (cnt + 1.0))
            }
        } else {
            self.load.add(-container.cpu_share);
        }
        self.last_update = time;
        self.reschedule_end();
    }
}

/// ShareManager dynamically manages CPU share of running containers.
pub trait ShareManager {
    /// Redistribute CPU share among running containers.
    fn redistribute(&mut self, mgr: &mut ContainerManager);
}

/// ConstantShareManager does not redistribute CPU share.
pub struct ConstantShareManager {}

impl ShareManager for ConstantShareManager {
    fn redistribute(&mut self, mgr: &mut ContainerManager) {}
}

pub struct CPU {
    pub cores: u32,
    pub share_manager: Box<dyn ShareManager>,
    pub progress_computer: ProgressComputer,
}

impl CPU {
    pub fn new(cores: u32, share_manager: Option<Box<dyn ShareManager>>, ctx: Rc<RefCell<SimulationContext>>) -> Self {
        let progress_computer = ProgressComputer {
            work_tree: Default::default(),
            work_map: Default::default(),
            work_total: Default::default(),
            cores: cores as f64,
            ctx,
            load: Default::default(),
            last_update: 0.,
            end_event: None,
        };
        Self {
            cores,
            share_manager: share_manager.unwrap_or(Box::new(ConstantShareManager {})),
            progress_computer,
        }
    }
}

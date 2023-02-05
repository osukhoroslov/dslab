/// This file contains the CPU sharing model used in the simulator.
/// We use something similar to CPUShares in cgroups, but instead of shares we operate with (shares/core_shares),
/// i. e. if the container has 512 shares and each core amounts to 1024 shares, we say that the share of the container equals 0.5.
/// If the container allows concurrent invocations, each invocation gets an equal part of the container share.
/// The exact CPU model depends on the CPUPolicy chosen.
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::rc::Rc;

use dslab_core::context::SimulationContext;
use dslab_core::event::EventId;

use crate::container::Container;
use crate::event::InvocationEndEvent;
use crate::invocation::Invocation;

#[derive(Clone)]
pub struct WorkItem {
    finish: f64,
    id: usize,
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

/// CPUPolicy governs CPU sharing, computes invocation progress and manages invocation end events.
pub trait CPUPolicy {
    fn clean_copy(&self) -> Box<dyn CPUPolicy>;
    fn get_load(&self) -> f64;
    fn set_cores(&mut self, cores: f64);
    fn on_invocation_end(
        &mut self,
        invocation: &mut Invocation,
        container: &mut Container,
        time: f64,
        ctx: &mut SimulationContext,
    );
    fn on_new_invocation(
        &mut self,
        invocation: &mut Invocation,
        container: &mut Container,
        time: f64,
        ctx: &mut SimulationContext,
    );
}

/// This policy does not emulate CPU at all. All invocations work as if executed on some abstract
/// infinite-core CPU with no load issues.
#[derive(Default)]
pub struct IgnoredCPUPolicy {
    load: f64,
}

impl CPUPolicy for IgnoredCPUPolicy {
    fn clean_copy(&self) -> Box<dyn CPUPolicy> {
        Box::<Self>::default()
    }

    fn get_load(&self) -> f64 {
        self.load
    }

    fn set_cores(&mut self, _cores: f64) {}

    fn on_invocation_end(
        &mut self,
        _invocation: &mut Invocation,
        container: &mut Container,
        _time: f64,
        _ctx: &mut SimulationContext,
    ) {
        if container.invocations.is_empty() {
            self.load -= container.cpu_share;
        }
    }

    fn on_new_invocation(
        &mut self,
        invocation: &mut Invocation,
        container: &mut Container,
        _time: f64,
        ctx: &mut SimulationContext,
    ) {
        if container.invocations.len() == 1 {
            self.load += container.cpu_share;
        }
        ctx.emit_self(InvocationEndEvent { id: invocation.id }, invocation.duration);
    }
}

/// CPU shares of active containers should not exceed the number of cores. If this limit is
/// exceeded, extra invocations are stalled until some cores are freed.
/// This model currently ignores possible effects related to multiple concurrent invocations on the same
/// container.
#[derive(Default)]
pub struct IsolatedCPUPolicy {
    cores: f64,
    load: f64,
    invocation_map: HashMap<usize, Vec<(usize, f64)>>,
    queue: VecDeque<(usize, f64)>,
}

impl CPUPolicy for IsolatedCPUPolicy {
    fn clean_copy(&self) -> Box<dyn CPUPolicy> {
        Box::<Self>::default()
    }

    fn get_load(&self) -> f64 {
        self.load
    }

    fn set_cores(&mut self, cores: f64) {
        self.cores = cores;
    }

    fn on_invocation_end(
        &mut self,
        _invocation: &mut Invocation,
        container: &mut Container,
        _time: f64,
        ctx: &mut SimulationContext,
    ) {
        if container.invocations.is_empty() {
            self.load -= container.cpu_share;
            while let Some(item) = self.queue.pop_front() {
                if self.load + item.1 > self.cores + 1e-9 {
                    self.queue.push_front(item);
                    break;
                }
                self.load += item.1;
                let mut invs = self.invocation_map.remove(&item.0).unwrap();
                for inv in invs.drain(..) {
                    ctx.emit_self(InvocationEndEvent { id: inv.0 }, inv.1);
                }
            }
        }
    }

    fn on_new_invocation(
        &mut self,
        invocation: &mut Invocation,
        container: &mut Container,
        _time: f64,
        ctx: &mut SimulationContext,
    ) {
        if let Some(invs) = self.invocation_map.get_mut(&container.id) {
            invs.push((invocation.id, invocation.duration));
        } else if container.invocations.len() == 1 && self.load + container.cpu_share > self.cores + 1e-9 {
            self.invocation_map
                .insert(container.id, vec![(invocation.id, invocation.duration)]);
            self.queue.push_back((container.id, container.cpu_share));
        } else {
            if container.invocations.len() == 1 {
                self.load += container.cpu_share;
            }
            ctx.emit_self(InvocationEndEvent { id: invocation.id }, invocation.duration);
        }
    }
}

/// CPU shares of active containers may exceed the number of cores, in this case the
/// invocations are slowed down.
/// We assume that CPU sharing is fair: each invocation makes progress according to its share.
#[derive(Default)]
pub struct ContendedCPUPolicy {
    work_tree: BTreeSet<WorkItem>,
    work_map: HashMap<usize, WorkItem>,
    work_total: f64,
    cores: f64,
    load: f64,
    last_update: f64,
    end_event: Option<EventId>,
}

impl ContendedCPUPolicy {
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

    fn reschedule_end(&mut self, ctx: &mut SimulationContext) {
        if let Some(evt) = self.end_event {
            ctx.cancel_event(evt);
        }
        if !self.work_tree.is_empty() {
            let it = self.work_tree.iter().next().unwrap().clone();
            let delta = (it.finish - self.work_total).max(0.);
            self.end_event = Some(ctx.emit_self(
                InvocationEndEvent { id: it.id },
                delta * f64::max(self.cores, self.load),
            ));
        } else {
            self.end_event = None;
        }
    }

    fn remove_invocation(&mut self, id: usize) -> f64 {
        let it = self.work_map.remove(&id).unwrap();
        self.work_tree.remove(&it);
        it.finish - self.work_total
    }

    fn insert_invocation(&mut self, id: usize, remain: f64) {
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
}

impl CPUPolicy for ContendedCPUPolicy {
    fn clean_copy(&self) -> Box<dyn CPUPolicy> {
        Box::<Self>::default()
    }

    fn get_load(&self) -> f64 {
        self.load
    }

    fn set_cores(&mut self, cores: f64) {
        self.cores = cores;
    }

    fn on_new_invocation(
        &mut self,
        invocation: &mut Invocation,
        container: &mut Container,
        time: f64,
        ctx: &mut SimulationContext
    ) {
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
            invocation.duration / self.cores * (container.invocations.len() as f64),
        );
        self.last_update = time;
        self.reschedule_end(ctx);
    }

    fn on_invocation_end(
        &mut self,
        invocation: &mut Invocation,
        container: &mut Container,
        time: f64,
        ctx: &mut SimulationContext,
    ) {
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
        self.reschedule_end(ctx);
    }
}

pub fn default_cpu_policy_resolver(s: &str) -> Box<dyn CPUPolicy> {
    let lower = s.to_lowercase();
    if lower == "ignored" {
        Box::<IgnoredCPUPolicy>::default()
    } else if lower == "isolated" {
        Box::<IsolatedCPUPolicy>::default()
    } else if lower == "contended" {
        Box::<ContendedCPUPolicy>::default()
    } else {
        panic!("Can't resolve: {}", s);
    }
}

pub struct CPU {
    pub cores: u32,
    policy: Box<dyn CPUPolicy>,
    ctx: Rc<RefCell<SimulationContext>>,
}

impl CPU {
    pub fn new(cores: u32, mut policy: Box<dyn CPUPolicy>, ctx: Rc<RefCell<SimulationContext>>) -> Self {
        policy.set_cores(cores as f64);
        Self { cores, policy, ctx }
    }

    pub fn get_load(&self) -> f64 {
        self.policy.get_load()
    }

    pub fn on_new_invocation(&mut self, invocation: &mut Invocation, container: &mut Container, time: f64) {
        self.policy
            .on_new_invocation(invocation, container, time, &mut self.ctx.borrow_mut())
    }

    pub fn on_invocation_end(&mut self, invocation: &mut Invocation, container: &mut Container, time: f64) {
        self.policy
            .on_invocation_end(invocation, container, time, &mut self.ctx.borrow_mut())
    }
}

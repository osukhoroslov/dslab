/// A brief description of CPU sharing model used in this simulator.
/// We use something similar to CPUShares in cgroups, but instead of shares we operate with (shares/core_shares),
/// i. e. if the container has 512 shares and each core amounts to 1024 shares, we say that the share of the container equals 0.5.
/// If the container allows concurrent invocations, each invocation gets an equal part of the container share.
/// We assume that CPU sharing is fair: each invocation makes progress according to its share.
use std::boxed::Box;
use std::cell::RefCell;
use std::rc::Rc;

use rand::prelude::*;
use rand_pcg::Pcg64;

use simcore::context::SimulationContext;
use simcore::event::EventId;

use crate::container::{Container, ContainerManager};
use crate::event::InvocationEndEvent;
use crate::invocation::Invocation;

/// treap node that contains invocation workload
/// workload is defined as duration * share / cores
/// and workload completion speed is defined as share / max(sum share, cores)
struct InvNode {
    /// invocation id
    pub id: u64,
    /// workload/share of this invocation
    pub remain: f64,
    /// minimum workload/share of whole subtree
    pub next: (f64, u64),
    /// time delta to push into children
    pub push: f64,
    /// treap priority
    pub prior: u64,
    pub l: Option<Box<InvNode>>,
    pub r: Option<Box<InvNode>>,
}

impl InvNode {
    pub fn new(id: u64, remain: f64, prior: u64) -> Self {
        Self {
            id,
            remain,
            next: (remain, id),
            push: 0.,
            prior,
            l: None,
            r: None,
        }
    }

    pub fn shift_time(&mut self, t: f64) {
        self.remain -= t;
        self.next.0 -= t;
        self.push += t;
    }

    pub fn push(&mut self) {
        if self.push > 0.0 {
            if let Some(l) = &mut self.l {
                l.shift_time(self.push);
            }
            if let Some(r) = &mut self.r {
                r.shift_time(self.push);
            }
            self.push = 0.0;
        }
    }

    pub fn recalc(&mut self) {
        self.next = (self.remain, self.id);
        if let Some(l) = &self.l {
            if l.next.0 < self.next.0 {
                self.next = l.next;
            }
        }
        if let Some(r) = &self.r {
            if r.next.0 < self.next.0 {
                self.next = r.next;
            }
        }
    }
}

fn inv_node_split(mut node: Option<Box<InvNode>>, id: u64) -> (Option<Box<InvNode>>, Option<Box<InvNode>>) {
    if let Some(mut v) = node.take() {
        v.push();
        if id <= v.id {
            let (x, y) = inv_node_split(v.l, id);
            v.l = y;
            v.recalc();
            return (x, Some(v));
        } else {
            let (x, y) = inv_node_split(v.r, id);
            v.r = x;
            v.recalc();
            return (Some(v), y);
        }
    }
    (None, None)
}

fn inv_node_merge(mut l: Option<Box<InvNode>>, mut r: Option<Box<InvNode>>) -> Option<Box<InvNode>> {
    if let Some(v) = &mut l {
        v.push();
    }
    if let Some(v) = &mut r {
        v.push();
    }
    if l.is_none() || r.is_none() {
        if let Some(v) = l {
            return Some(v);
        }
        if let Some(v) = r {
            return Some(v);
        }
        return None;
    }
    let mut v = l.unwrap();
    let mut u = r.unwrap();
    if v.prior > u.prior {
        v.r = inv_node_merge(v.r, Some(u));
        v.recalc();
        Some(v)
    } else {
        u.l = inv_node_merge(Some(v), u.l);
        u.recalc();
        Some(u)
    }
}

/// ProgressComputer computes invocation progress
pub struct ProgressComputer {
    treap: Option<Box<InvNode>>,
    treap_rng: Pcg64,
    cores: f64,
    ctx: Rc<RefCell<SimulationContext>>,
    load: f64,
    last_update: f64,
    end_event: Option<EventId>,
}

impl ProgressComputer {
    fn reschedule_end(&mut self) {
        if let Some(evt) = self.end_event {
            self.ctx.borrow_mut().cancel_event(evt);
        }
        if let Some(v) = &self.treap {
            let (t, id) = v.next.clone();
            self.end_event = Some(
                self.ctx
                    .borrow_mut()
                    .emit_self(InvocationEndEvent { id }, t * f64::max(self.cores, self.load)),
            );
        } else {
            self.end_event = None;
        }
    }

    fn remove_invocation(&mut self, id: u64) -> f64 {
        let (l, tmp) = inv_node_split(self.treap.take(), id);
        let (v, r) = inv_node_split(tmp, id + 1);
        self.treap = inv_node_merge(l, r);
        v.unwrap().remain
    }

    fn insert_invocation(&mut self, id: u64, remain: f64) {
        let (l, r) = inv_node_split(self.treap.take(), id);
        let v = Box::new(InvNode::new(id, remain, self.treap_rng.gen::<u64>()));
        self.treap = inv_node_merge(inv_node_merge(l, Some(v)), r);
    }

    fn transform_time(&self, time: f64, share: f64, forward: bool) -> f64 {
        if forward {
            share * time / self.cores
        } else {
            time * self.cores / share
        }
    }

    fn shift_time(&mut self, time: f64) {
        if let Some(v) = &mut self.treap {
            v.shift_time((time - self.last_update) / f64::max(self.cores, self.load));
        }
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
            self.load += container.cpu_share;
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
        self.remove_invocation(invocation.id);
        self.shift_time(time);
        if container.invocations.len() > 0 {
            let cnt = container.invocations.len() as f64;
            for i in container.invocations.iter().copied() {
                let remain = self.remove_invocation(i);
                self.insert_invocation(i, remain * cnt / (cnt + 1.0))
            }
        } else {
            self.load -= container.cpu_share;
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
    pub fn new(
        cores: u32,
        share_manager: Option<Box<dyn ShareManager>>,
        ctx: Rc<RefCell<SimulationContext>>,
        random_seed: u64,
    ) -> Self {
        let progress_computer = ProgressComputer {
            treap: None,
            treap_rng: Pcg64::seed_from_u64(random_seed),
            cores: cores as f64,
            ctx,
            load: 0.,
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

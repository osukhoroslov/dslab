use log::info;
use std::collections::HashSet;

use core::actor::{Actor, ActorContext, ActorId, Event};
use core::cast;

use crate::common::AllocationVerdict;
use crate::events::{
    AllocationCommitFailed, AllocationCommitRequest, AllocationCommitSucceeded, AllocationFailed, AllocationReleased,
    AllocationRequest,
};
use crate::network::MESSAGE_DELAY;
use crate::resource_pool::ResourcePoolState;
use crate::vm::VirtualMachine;

#[derive(Debug, Clone)]
pub struct PlacementStore {
    state: ResourcePoolState,
    schedulers: HashSet<String>,
}

impl PlacementStore {
    pub fn new() -> Self {
        Self {
            state: ResourcePoolState::new(),
            schedulers: HashSet::new(),
        }
    }

    pub fn add_host(&mut self, id: &str, cpu_total: u32, memory_total: u64) {
        self.state
            .add_host(id, cpu_total, memory_total, cpu_total, memory_total);
    }

    pub fn add_scheduler(&mut self, id: &str) {
        self.schedulers.insert(id.to_string());
    }

    pub fn get_state(&self) -> ResourcePoolState {
        self.state.clone()
    }

    fn on_allocation_commit_request(
        &mut self,
        vm: &VirtualMachine,
        host_id: &String,
        scheduler: ActorId,
        ctx: &mut ActorContext,
    ) {
        if self.state.can_allocate(vm, host_id) == AllocationVerdict::Success {
            self.state.place_vm(vm, host_id);
            info!(
                "[time = {}] vm #{} commited to host #{} in placement store",
                ctx.time(),
                vm.id,
                host_id
            );
            ctx.emit(
                AllocationRequest { vm: vm.clone() },
                ActorId::from(host_id),
                MESSAGE_DELAY,
            );

            for host in self.schedulers.iter() {
                ctx.emit(
                    AllocationCommitSucceeded {
                        vm: vm.clone(),
                        host_id: host_id.to_string(),
                    },
                    ActorId::from(&host),
                    MESSAGE_DELAY,
                );
            }
        } else {
            info!(
                "[time = {}] not enough space for vm #{} on host #{} in placement store",
                ctx.time(),
                vm.id,
                host_id
            );
            ctx.emit(
                AllocationCommitFailed {
                    vm: vm.clone(),
                    host_id: host_id.to_string(),
                },
                scheduler.clone(),
                MESSAGE_DELAY,
            );
        }
    }

    fn on_allocation_failed(&mut self, vm: &VirtualMachine, host_id: &String, ctx: &mut ActorContext) {
        self.state.remove_vm(vm, host_id);

        for scheduler in self.schedulers.iter() {
            ctx.emit(
                AllocationReleased {
                    vm: vm.clone(),
                    host_id: host_id.to_string(),
                },
                ActorId::from(&scheduler),
                MESSAGE_DELAY,
            );
        }
    }

    fn on_allocation_released(&mut self, vm: &VirtualMachine, host_id: &String, ctx: &mut ActorContext) {
        self.state.remove_vm(vm, host_id);

        for scheduler in self.schedulers.iter() {
            ctx.emit(
                AllocationReleased {
                    vm: vm.clone(),
                    host_id: host_id.to_string(),
                },
                ActorId::from(&scheduler),
                MESSAGE_DELAY,
            );
        }
    }
}

impl Actor for PlacementStore {
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            AllocationCommitRequest { vm, host_id } => {
                self.on_allocation_commit_request(vm, host_id, from, ctx)
            }
            AllocationFailed { vm, host_id } => {
                self.on_allocation_failed(vm, host_id, ctx)
            }
            AllocationReleased { vm, host_id } => {
                self.on_allocation_released(vm, host_id, ctx)
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}

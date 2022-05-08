use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::rc::Rc;
use sugars::{rc, refcell};

use serde::Serialize;

use crate::core::common::VmStatus;
use crate::core::events::allocation::MigrationRequest;
use crate::core::monitoring::HostState;
use crate::core::monitoring::Monitoring;
use crate::core::resource_pool::Allocation;
use crate::core::vm::VirtualMachine;
use crate::custom_component::CustomComponent;
use simcore::cast;
use simcore::context::SimulationContext;
use simcore::event::Event;
use simcore::handler::EventHandler;
use simcore::log_debug;
use simcore::log_info;
use simcore::log_warn;

#[derive(Serialize)]
pub struct PerformMigrations {}

pub struct VmMigrator {
    interval: f64,

    #[allow(dead_code)]
    overload_threshold: f64,

    underload_threshold: f64,
    monitoring: Option<Rc<RefCell<Monitoring>>>,
    allocations: Rc<RefCell<HashMap<u32, Allocation>>>,
    vms: Rc<RefCell<BTreeMap<u32, VirtualMachine>>>,
    ctx: SimulationContext,
}

impl VmMigrator {
    pub fn patch_custom_args(
        &mut self,
        interval: f64,
        monitoring: Rc<RefCell<Monitoring>>,
        allocations: Rc<RefCell<HashMap<u32, Allocation>>>,
        vms: Rc<RefCell<BTreeMap<u32, VirtualMachine>>>,
    ) {
        self.interval = interval;
        self.monitoring = Some(monitoring.clone());
        self.allocations = allocations.clone();
        self.vms = vms.clone();
    }

    fn schedule_migration(&mut self, vm_id: u32, target_host: u32) {
        let start_time = self.vms.borrow_mut().get_mut(&vm_id).unwrap().start_time;
        self.vms.borrow_mut().get_mut(&vm_id).unwrap().lifetime -= self.ctx.time() - start_time;
        let source_host = self.monitoring.clone().unwrap().borrow_mut().find_host_by_vm(vm_id);
        log_info!(
            self.ctx,
            "schedule migration of vm {} from host {} to host {}",
            vm_id,
            source_host,
            target_host
        );

        self.ctx.emit(
            MigrationRequest {
                source_host: source_host.clone(),
                alloc: self.allocations.borrow().get(&vm_id).unwrap().clone(),
                vm: self.vms.borrow_mut().get_mut(&vm_id).unwrap().clone(),
            },
            target_host,
            0.5,
        );
    }

    fn perform_migrations(&mut self) {
        if self.monitoring.is_none() {
            log_warn!(self.ctx, "cannot perform migrations as there`s no monitoring");
            self.ctx.emit_self(PerformMigrations {}, self.interval);
            return;
        }

        log_debug!(self.ctx, "perform migrations");

        // build host states
        let mut host_states = BTreeMap::<u32, HostState>::new();
        let mut allocations = HashMap::new();
        allocations.extend(self.allocations.borrow().clone().into_iter());
        for host in self.monitoring.clone().unwrap().borrow().get_hosts_list() {
            let state: HostState = self.monitoring.clone().unwrap().borrow().get_host_state(*host).clone();
            host_states.insert(*host, state);
        }

        // select underloaded VMs to migrate ===================================

        let mut vms_to_migrate = Vec::<u32>::new();
        let mut min_load: f64 = 1.;
        for host in host_states.clone() {
            let state = &host_states[&host.0];
            if state.cpu_load == 0. {
                // host turned off
                continue;
            }
            if state.cpu_load < self.underload_threshold || state.memory_load < self.underload_threshold {
                min_load = min_load.min(state.cpu_load).min(state.memory_load);
                for vm_id in self.monitoring.clone().unwrap().borrow().get_host_vms(host.0) {
                    let vm_status = self.monitoring.clone().unwrap().borrow().vm_status(vm_id);
                    if vm_status != VmStatus::Running {
                        continue;
                    }
                    vms_to_migrate.push(vm_id);
                }

                let new_state = HostState {
                    cpu_load: 0.,
                    memory_load: 0.,
                    cpu_total: state.cpu_total,
                    memory_total: state.memory_total,
                };
                host_states.insert(host.0, new_state);
            }
        }

        // build migration schema using Best Fit ===============================

        // target hosts, cannot migrate from them if they are underloaded
        let mut target_hosts = HashSet::<u32>::new();

        for vm_id in vms_to_migrate {
            let current_host = self.monitoring.clone().unwrap().borrow_mut().find_host_by_vm(vm_id);
            if target_hosts.contains(&current_host) {
                continue;
            }

            let mut best_host: Option<u32> = None;
            let mut best_cpu_load = 0.;
            let mut best_memory_load = 0.;

            for host in host_states.clone() {
                if host.0 == current_host {
                    continue;
                }

                let state = self.monitoring.clone().unwrap().borrow().get_host_state(host.0).clone();
                if state.cpu_load < min_load && state.memory_load < min_load {
                    continue;
                }

                let cpu_usage = state.cpu_load * (state.cpu_total as f64);
                let memory_usage = state.memory_load * (state.memory_total as f64);
                let cpu_load_new = (cpu_usage + (allocations[&vm_id].cpu_usage as f64)) / (state.cpu_total as f64);
                let memory_load_new =
                    (memory_usage + (allocations[&vm_id].memory_usage as f64)) / (state.memory_total as f64);
                if cpu_load_new < self.overload_threshold && memory_load_new < self.overload_threshold {
                    if cpu_load_new > best_cpu_load {
                        best_cpu_load = cpu_load_new;
                        best_memory_load = memory_load_new;
                        best_host = Some(host.0);
                    }
                }
            }

            if best_host.is_some() {
                let state = &host_states[&best_host.unwrap()];
                let new_state = HostState {
                    cpu_load: best_cpu_load,
                    memory_load: best_memory_load,
                    cpu_total: state.cpu_total,
                    memory_total: state.memory_total,
                };

                host_states.insert(best_host.unwrap(), new_state);
                target_hosts.insert(best_host.unwrap());
                self.schedule_migration(vm_id, best_host.unwrap());
            }
        }

        // schedule new migration attempt ======================================
        self.ctx.emit_self(PerformMigrations {}, self.interval);
    }
}

impl CustomComponent for VmMigrator {
    fn new(ctx: SimulationContext) -> Self {
        Self {
            interval: 1.,
            overload_threshold: 0.8,
            underload_threshold: 0.4,
            monitoring: None,
            allocations: rc!(refcell!(HashMap::<u32, Allocation>::new())),
            vms: rc!(refcell!(BTreeMap::<u32, VirtualMachine>::new())),
            ctx,
        }
    }

    fn init(&mut self) {
        self.ctx.emit_self(PerformMigrations {}, 0.);
    }
}

impl EventHandler for VmMigrator {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            PerformMigrations {} => {
                self.perform_migrations();
            }
        })
    }
}

use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;

use serde::Serialize;

use crate::core::config::SimulationConfig;
use crate::core::events::allocation::MigrationRequest;
use crate::core::monitoring::Monitoring;
use crate::core::vm::VmStatus;
use crate::core::vm_api::VmAPI;
use crate::custom_component::CustomComponent;
use dslab_core::cast;
use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;
use dslab_core::{log_debug, log_info, log_trace, log_warn};

#[derive(Serialize)]
pub struct PerformMigrations {}

pub struct VmMigrator {
    interval: f64,
    overload_threshold: f64,
    underload_threshold: f64,
    monitoring: Option<Rc<RefCell<Monitoring>>>,
    vm_api: Option<Rc<RefCell<VmAPI>>>,
    sim_config: Option<Rc<SimulationConfig>>,
    ctx: SimulationContext,
}

impl VmMigrator {
    pub fn patch_custom_args(
        &mut self,
        interval: f64,
        monitoring: Rc<RefCell<Monitoring>>,
        vm_api: Rc<RefCell<VmAPI>>,
        sim_config: Rc<SimulationConfig>,
    ) {
        self.interval = interval;
        self.monitoring = Some(monitoring);
        self.vm_api = Some(vm_api);
        self.sim_config = Some(sim_config);
    }

    fn perform_migrations(&mut self) {
        if self.monitoring.is_none() {
            log_warn!(self.ctx, "cannot perform migrations as there is no monitoring");
            self.ctx.emit_self(PerformMigrations {}, self.interval);
            return;
        } else if self.vm_api.is_none() {
            log_warn!(self.ctx, "cannot perform migrations as there is no VM API");
            self.ctx.emit_self(PerformMigrations {}, self.interval);
            return;
        } else {
            log_trace!(self.ctx, "perform migrations");
        }

        let vm_api = self.vm_api.as_ref().unwrap().borrow();
        let mon = self.monitoring.as_ref().unwrap().borrow();
        let mut host_states = mon.get_host_states().clone();

        // select VMs to migrate ---------------------------------------------------------------------------------------

        let mut vms_to_migrate = Vec::<(u32, u32)>::new();
        let mut overloaded_hosts = Vec::<u32>::new();
        let mut min_load: f64 = 1.;
        for (host, state) in host_states.iter() {
            // host is not active
            if state.cpu_load == 0. {
                continue;
            }
            // host is underloaded
            if state.cpu_load < self.underload_threshold || state.memory_load < self.underload_threshold {
                log_debug!(
                    self.ctx,
                    "host {} is underloaded ({} load, {} vms)",
                    host,
                    state.cpu_load,
                    state.vms.len()
                );
                min_load = min_load.min(state.cpu_load).min(state.memory_load);
                for vm_id in state.vms.iter() {
                    let status = vm_api.get_vm_status(*vm_id);
                    if status != VmStatus::Running {
                        continue;
                    }
                    vms_to_migrate.push((*vm_id, *host));
                }
            }
            // host is overloaded
            if state.cpu_load > self.overload_threshold || state.memory_load > self.overload_threshold {
                log_debug!(
                    self.ctx,
                    "host {} is overloaded ({} load, {} vms)",
                    host,
                    state.cpu_load,
                    state.vms.len()
                );
                overloaded_hosts.push(*host);
                let mut cpu_usage = state.cpu_load * (state.cpu_total as f64);
                let mut memory_usage = state.memory_load * (state.memory_total as f64);

                for vm_id in state.vms.iter() {
                    let status = vm_api.get_vm_status(*vm_id);
                    let vm = vm_api.get_vm(*vm_id).borrow().clone();
                    if status != VmStatus::Running {
                        continue;
                    }
                    vms_to_migrate.push((*vm_id, *host));

                    cpu_usage -= vm.cpu_usage as f64;
                    memory_usage -= vm.memory_usage as f64;
                    let new_cpu_load = cpu_usage / (state.cpu_total as f64);
                    let new_memory_load = memory_usage / (state.memory_total as f64);

                    if new_cpu_load <= self.overload_threshold && new_memory_load <= self.overload_threshold {
                        break;
                    }
                }
            }
        }

        // migrate VMs using Best Fit ----------------------------------------------------------------------------------

        if vms_to_migrate.len() > 0 {
            log_debug!(self.ctx, "try to migrate {} vms", vms_to_migrate.len());
        }

        // target hosts, cannot migrate from them as some VM(s) are migrating and will increase their load rate
        let mut target_hosts = HashSet::<u32>::new();
        let mut source_hosts = HashSet::<u32>::new();

        for (vm_id, source_host) in vms_to_migrate {
            if target_hosts.contains(&source_host) {
                continue;
            }
            let mut target_host_opt: Option<u32> = None;
            let mut best_cpu_load = 0.;
            let vm = vm_api.get_vm(vm_id).borrow().clone();

            for (host, state) in host_states.iter() {
                if *host == source_host {
                    continue;
                }
                // do not use source hosts as targets
                if source_hosts.contains(host) {
                    continue;
                }
                // do not use low loaded hosts as targets? (unless source is overloaded)
                if !overloaded_hosts.contains(&source_host)
                    && min_load < 1.
                    && (state.cpu_load < min_load && state.memory_load < min_load)
                {
                    continue;
                }

                let source_state = host_states.get(&source_host).unwrap();
                let cpu_usage_source = source_state.cpu_load * (source_state.cpu_total as f64);
                let cpu_load_new_source = (cpu_usage_source - vm.cpu_usage as f64) / (source_state.cpu_total as f64);

                let cpu_usage_target = state.cpu_load * (state.cpu_total as f64);
                let memory_usage_target = state.memory_load * (state.memory_total as f64);
                let cpu_load_new_target = (cpu_usage_target + vm.cpu_usage as f64) / (state.cpu_total as f64);
                let memory_load_new_target =
                    (memory_usage_target + vm.memory_usage as f64) / (state.memory_total as f64);

                if !overloaded_hosts.contains(&source_host)
                    && source_state.cpu_load > state.cpu_load
                    && cpu_load_new_source < cpu_load_new_target
                {
                    continue;
                }

                if cpu_load_new_target < self.overload_threshold && memory_load_new_target < self.overload_threshold {
                    if cpu_load_new_target > best_cpu_load {
                        best_cpu_load = cpu_load_new_target;
                        target_host_opt = Some(*host);
                    }
                }
            }

            if let Some(target_host) = target_host_opt {
                source_hosts.insert(source_host);
                target_hosts.insert(target_host);

                // schedule migration
                log_info!(
                    self.ctx,
                    "schedule migration of vm {} from host {} to host {}",
                    vm_id,
                    source_host,
                    target_host
                );
                let vm = vm_api.get_vm(vm_id).borrow().clone();
                self.ctx.emit(
                    MigrationRequest {
                        source_host,
                        vm_id: vm.id,
                    },
                    target_host,
                    self.sim_config.as_ref().unwrap().message_delay,
                );

                // update source host state
                let source_state = host_states.get_mut(&source_host).unwrap();
                let cpu_usage = source_state.cpu_load * (source_state.cpu_total as f64);
                let memory_usage = source_state.memory_load * (source_state.memory_total as f64);
                let cpu_load_new = (cpu_usage - vm.cpu_usage as f64) / (source_state.cpu_total as f64);
                let memory_load_new = (memory_usage + vm.memory_usage as f64) / (source_state.memory_total as f64);
                source_state.cpu_load = cpu_load_new;
                source_state.memory_load = memory_load_new;

                // update target host state
                let target_state = host_states.get_mut(&target_host).unwrap();
                let cpu_usage = target_state.cpu_load * (target_state.cpu_total as f64);
                let memory_usage = target_state.memory_load * (target_state.memory_total as f64);
                let cpu_load_new = (cpu_usage + vm.cpu_usage as f64) / (target_state.cpu_total as f64);
                let memory_load_new = (memory_usage + vm.memory_usage as f64) / (target_state.memory_total as f64);
                target_state.cpu_load = cpu_load_new;
                target_state.memory_load = memory_load_new;
            } else {
                log_debug!(
                    self.ctx,
                    "no suitable target to migrate vm {} from host {}",
                    vm_id,
                    source_host
                );
            }
        }

        // schedule the next migration attempt
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
            vm_api: None,
            sim_config: None,
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

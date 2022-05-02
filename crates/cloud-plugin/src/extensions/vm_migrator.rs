use std::cell::RefCell;
use std::rc::Rc;

use serde::Serialize;

use simcore::cast;
use simcore::context::SimulationContext;
use simcore::event::Event;
use simcore::handler::EventHandler;
use simcore::log_debug;

use crate::core::monitoring::Monitoring;
use crate::custom_component::CustomComponent;

#[derive(Serialize)]
pub struct PerformMigrations {}

pub struct VmMigrator {
    interval: f64,
    monitoring: Option<Rc<RefCell<Monitoring>>>,
    ctx: SimulationContext,
}

impl VmMigrator {
    pub fn patch_custom_args(&mut self, interval: f64, monitoring: Rc<RefCell<Monitoring>>) {
        self.interval = interval;
        self.monitoring = Some(monitoring.clone());
    }

    fn perform_migrations(&mut self) {
        log_debug!(self.ctx, "perform migrations");

        // TODO add migrator logic here

        self.ctx.emit_self(PerformMigrations {}, self.interval);
    }
}

impl CustomComponent for VmMigrator {
    fn new(ctx: SimulationContext) -> Self {
        Self {
            interval: 0.,
            monitoring: None,
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

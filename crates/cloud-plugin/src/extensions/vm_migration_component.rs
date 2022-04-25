use std::cell::RefCell;
use std::rc::Rc;

use serde::Serialize;
use sugars::{rc, refcell};

use simcore::cast;
use simcore::context::SimulationContext;
use simcore::event::Event;
use simcore::handler::EventHandler;
use simcore::log_debug;

use crate::core::monitoring::Monitoring;
use crate::custom_component::CustomComponent;

#[derive(Serialize)]
pub struct PerformMigrations {}

pub struct VmMigrationComponentHandler {
    interval: f64,
    monitoring: Option<Rc<RefCell<Monitoring>>>,
    ctx: SimulationContext,
}

impl VmMigrationComponentHandler {
    pub fn new(ctx: SimulationContext) -> Self {
        Self {
            interval: 0.,
            monitoring: None,
            ctx,
        }
    }

    pub fn perform_migrations(&mut self) {
        log_debug!(self.ctx, "perform migrations");

        // TODO add migrator logic here

        self.ctx.emit_self(PerformMigrations {}, self.interval);
    }
}

impl EventHandler for VmMigrationComponentHandler {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            PerformMigrations {} => {
                self.perform_migrations();
            }
        })
    }
}

pub struct VmMigrationComponent {
    handler: Rc<RefCell<VmMigrationComponentHandler>>,
}

impl VmMigrationComponent {
    pub fn patch_custom_args(&mut self, interval: f64, monitoring: Rc<RefCell<Monitoring>>) {
        self.handler.borrow_mut().interval = interval;
        self.handler.borrow_mut().monitoring = Some(monitoring.clone());
    }
}

impl CustomComponent for VmMigrationComponent {
    fn new(ctx: SimulationContext) -> Self {
        Self {
            handler: rc!(refcell!(VmMigrationComponentHandler::new(ctx,))),
        }
    }

    fn handler(&self) -> Rc<RefCell<dyn EventHandler>> {
        return self.handler.clone();
    }

    fn init(&mut self) {
        self.handler.borrow_mut().ctx.emit_self(PerformMigrations {}, 0.);
    }
}

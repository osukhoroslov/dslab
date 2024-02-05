use dslab_compute::multicore::CoresDependency;
use log::log_enabled;
use log::Level::Info;
use rand::prelude::*;
use rand_pcg::Pcg64;
use serde::Serialize;
use std::cell::RefCell;
use std::rc::Rc;

use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;
use dslab_core::{cast, log_debug};
use dslab_network::Network;

use crate::common::Start;
use crate::job::*;
use crate::server::ServerRegister;

const BATCH_SIZE: u32 = 5;
const JOBS_AMOUNT_TOTAL: u32 = 20;

#[derive(Clone, Serialize)]
pub struct ReportStatus {}

#[derive(Clone, Serialize)]
pub struct GenerateJobs {}

pub struct JobGenerator {
    id: Id,
    net: Rc<RefCell<Network>>,
    server_id: Option<Id>,
    jobs_generated: u32,
    ctx: SimulationContext,
}

impl JobGenerator {
    pub fn new(net: Rc<RefCell<Network>>, ctx: SimulationContext) -> Self {
        Self {
            id: ctx.id(),
            net,
            server_id: None,
            jobs_generated: 0,
            ctx,
        }
    }

    fn on_started(&mut self) {
        log_debug!(self.ctx, "started");
        self.ctx.emit_self(GenerateJobs {}, 1.);
        if log_enabled!(Info) {
            self.ctx.emit_self(ReportStatus {}, 100.);
        }
    }

    fn on_server_register(&mut self, server_id: Id) {
        log_debug!(self.ctx, "registered server: {:?}", server_id);
        self.server_id = Some(server_id);
    }

    fn generate_jobs(&mut self) {
        if self.server_id.is_none() {
            return;
        }
        let mut rand = Pcg64::seed_from_u64(42);
        for i in 0..BATCH_SIZE {
            let job = JobRequest {
                id: (self.jobs_generated + i) as u64,
                flops: rand.gen_range(100..=1000) as f64,
                memory: rand.gen_range(1..=8) * 128,
                min_cores: 1,
                max_cores: 1,
                cores_dependency: CoresDependency::Linear,
                input_size: rand.gen_range(100..=1000),
                output_size: rand.gen_range(10..=100),
            };
            self.net.borrow_mut().send_event(job, self.id, self.server_id.unwrap());
        }
        self.jobs_generated += BATCH_SIZE;
        if self.jobs_generated < JOBS_AMOUNT_TOTAL {
            self.ctx.emit_self(GenerateJobs {}, 20.);
        }
    }
}

impl EventHandler for JobGenerator {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            Start {} => {
                self.on_started();
            }
            GenerateJobs {} => {
                self.generate_jobs();
            }
            ServerRegister {} => {
                self.on_server_register(event.src);
            }
        })
    }
}

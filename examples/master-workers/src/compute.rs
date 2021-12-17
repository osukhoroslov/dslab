use core::actor::{Actor, ActorContext, ActorId, Event};
use core::cast;
use std::collections::BTreeMap;

#[derive(Debug)]
pub struct CompRequest {
    pub size: u64,
    pub requester: ActorId,
}

#[derive(Debug)]
pub struct CompFinished {
    pub id: u64,
}

pub struct Compute {
    id: ActorId,
    speed: u64,
    cpus: u32,
    comps: BTreeMap<u64, CompRequest>,
}

impl Compute {
    pub fn new(id: &str, speed: u64, cpus: u32) -> Self {
        Self {
            id: ActorId::from(id),
            speed,
            cpus,
            comps: BTreeMap::new(),
        }
    }

    pub fn speed(&self) -> u64 {
        self.speed
    }

    pub fn cpus(&self) -> u32 {
        self.cpus
    }

    pub fn run(&self, size: u64, ctx: &mut ActorContext) -> u64 {
        let req = CompRequest {
            size,
            requester: ctx.id.clone(),
        };
        ctx.emit_now(req, self.id.clone())
    }
}

impl Actor for Compute {
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            CompRequest { size, requester: _ } => {
                let comp_id = ctx.event_id;
                println!("{} [{}] comp {} started: {:?}", ctx.time(), ctx.id, comp_id, event);
                let comp_time = *size as f64 / self.speed as f64;
                ctx.emit(CompFinished { id: comp_id }, from.clone(), comp_time);
                self.comps.insert(comp_id, *event.downcast::<CompRequest>().unwrap());
            }
            CompFinished { id } => {
                let comp = self.comps.remove(id).unwrap();
                println!("{} [{}] comp {} finished: {:?}", ctx.time(), ctx.id, *id, comp);
                ctx.emit_now(CompFinished { id: *id }, comp.requester);
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}

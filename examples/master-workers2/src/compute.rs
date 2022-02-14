use core2::context::SimulationContext;

#[derive(Debug)]
pub struct CompFinished {
    pub id: u64,
}

pub struct Compute {
    speed: u64,
    cpus_total: u32,
    memory_total: u64,
    next_id: u64,
    ctx: SimulationContext,
}

impl Compute {
    pub fn new(speed: u64, cpus_total: u32, memory_total: u64, ctx: SimulationContext) -> Self {
        Self {
            speed,
            cpus_total,
            memory_total,
            next_id: 0,
            ctx,
        }
    }

    pub fn speed(&self) -> u64 {
        self.speed
    }

    pub fn cpus_total(&self) -> u32 {
        self.cpus_total
    }

    pub fn memory_total(&self) -> u64 {
        self.memory_total
    }

    pub fn run<S: Into<String>>(&mut self, size: u64, requester: S) -> u64 {
        let comp_id = self.next_id;
        self.next_id += 1;
        let comp_time = size as f64 / self.speed as f64;
        self.ctx.emit(CompFinished { id: comp_id }, requester, comp_time);
        comp_id
    }
}

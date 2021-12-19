use core::match_event;
use core::actor::{ActorId, Actor, ActorContext, Event};

// ACTORS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct Monitoring {
    pub id: ActorId,
    hosts: Vec<ActorId>,
    host_cpu_available: Vec<u32>,
    host_ram_available: Vec<u32>,
}

impl Monitoring {
    pub fn new(id: ActorId) -> Self {
        Self {
            id: id,
            hosts: Vec::new(),
            host_cpu_available: Vec::new(),
            host_ram_available: Vec::new()
        }
    }

    pub fn add_host(&mut self, host: ActorId) {
        self.hosts.push(host.clone());
        self.host_cpu_available.push(0);
        self.host_ram_available.push(0);
    }

    pub fn cpu_available(&self, i: usize) -> u32 {
        return self.host_cpu_available[i];
    }

    pub fn ram_available(&self, i: usize) -> u32 {
        return self.host_ram_available[i];
    }

    pub fn number_of_hosts(&self) -> usize {
        return self.hosts.len();
    }

    pub fn get_host_actor_id(&self, i: usize) -> ActorId {
        return self.hosts[i].clone();
    }
}

// EVENTS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct UpdateHostStats {
    pub host_id: ActorId,
    pub cpu_available: u32,
    pub ram_available: u32,
}

impl Actor for Monitoring {
    fn on(&mut self, event: Box<dyn Event>, 
                     _from: ActorId, ctx: &mut ActorContext) {
        match_event!( event {
            UpdateHostStats { host_id, cpu_available, ram_available } => {
                println!("[time = {}] monitoring received stats from host #{}",
                    ctx.time(), host_id
                );
                let mut found = false;
                for i in 0..self.hosts.len() {
                    if *host_id == self.hosts[i] {
                        self.host_cpu_available[i] = *cpu_available;
                        self.host_ram_available[i] = *ram_available;
                        found = true;
                        break;
                    }
                }
                if !found {
                    self.add_host(host_id.clone());
                    self.host_cpu_available[ self.hosts.len() - 1 ] = *cpu_available;
                    self.host_ram_available[ self.hosts.len() - 1 ] = *ram_available;
                }
            },
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}

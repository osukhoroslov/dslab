use dslab_core::{async_mode::UnboundedQueue, Simulation};

struct Data {
    value: u32,
}

#[test]
fn test_simple_queue() {
    let mut sim = Simulation::new(123);
    let queue = sim.create_queue("queue");
}

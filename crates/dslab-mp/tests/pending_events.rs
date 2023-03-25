use rand::prelude::IteratorRandom;

use dslab_mp::mc::events::{McEvent, McTime};
use dslab_mp::mc::pending_events::PendingEvents;

#[test]
fn test_mc_time() {
    let a = McTime::from(0.0);
    let b = McTime::from(0.0);
    assert!(b <= a);
    assert!(a <= b);
    assert!(a == b);
}

#[test]
fn test_dependency_resolver_simple() {
    let mut resolver = PendingEvents::new();
    let mut sequence = Vec::new();
    let mut rev_id = vec![0; 9];
    for node_id in 0..3 {
        let times: Vec<u64> = (0..3).into_iter().collect();
        for event_time in times {
            let event = McEvent::TimerFired {
                proc: node_id.to_string(),
                timer: format!("{}", event_time),
                timer_delay: McTime::from(event_time as f64),
            };
            rev_id[resolver.push(event)] = event_time * 3 + node_id;
        }
    }
    println!("{:?}", rev_id);
    while let Some(id) = resolver.available_events().iter().choose(&mut rand::thread_rng()) {
        let id = *id;
        sequence.push(rev_id[id]);
        resolver.pop(id);
    }
    println!("{:?}", sequence);
    assert!(sequence.len() == 9);
    let mut timers = vec![0, 0, 0];
    for event_id in sequence {
        let time = event_id / 3;
        let node = event_id % 3;
        assert!(timers[node as usize] == time);
        timers[node as usize] += 1;
    }
}

#[test]
fn test_dependency_resolver_pop() {
    let mut resolver = PendingEvents::new();
    let mut sequence = Vec::new();
    let mut rev_id = vec![0; 12];

    for node_id in 0..3 {
        let times: Vec<u64> = (0..3).into_iter().collect();
        for event_time in times {
            let event = McEvent::TimerFired {
                proc: node_id.to_string(),
                timer: format!("{}", event_time),
                timer_delay: McTime::from(1.0 + event_time as f64),
            };
            rev_id[resolver.push(event)] = event_time * 3 + node_id;
        }
    }

    // remove most of elements
    // timer resolver should clear its queues before it
    // can add next events without broken dependencies
    // every process moved its global timer at least once
    for _ in 0..7 {
        let id = *resolver
            .available_events()
            .iter()
            .choose(&mut rand::thread_rng())
            .unwrap();
        sequence.push(rev_id[id]);
        resolver.pop(id);
    }

    // these events will be last
    for node_id in 0..3 {
        let event = McEvent::TimerFired {
            proc: node_id.to_string(),
            timer: format!("{}", node_id),
            timer_delay: McTime::from(2.1),
        };
        rev_id[resolver.push(event)] = 9 + node_id;
    }
    while let Some(id) = resolver.available_events().iter().choose(&mut rand::thread_rng()) {
        let id = *id;
        sequence.push(rev_id[id]);
        resolver.pop(id);
    }
    println!("{:?}", sequence);
    assert!(sequence.len() == 12);
    let mut timers = vec![0, 0, 0];
    for event_id in sequence {
        let time = event_id / 3;
        let node = event_id % 3;
        assert!(timers[node as usize] == time);
        timers[node as usize] += 1;
    }
}

use rand::prelude::IteratorRandom;

use dslab_mp::mc::events::{McEvent, McTime};
use dslab_mp::mc::pending_events::PendingEvents;

#[test]
fn test_mc_time() {
    let a = McTime::from(0.0);
    let b = McTime::from(0.0);
    assert!(b <= a);
    assert!(a <= b);
    assert_eq!(a, b);
}

#[test]
fn test_dependency_resolver_simple() {
    let mut pending_events = PendingEvents::new();
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
            rev_id[pending_events.push(event)] = event_time * 3 + node_id;
        }
    }
    println!("{:?}", rev_id);
    while let Some(id) = pending_events.available_events().iter().choose(&mut rand::thread_rng()) {
        let id = *id;
        sequence.push(rev_id[id]);
        pending_events.pop(id);
    }
    println!("{:?}", sequence);
    assert_eq!(sequence.len(), 9);
    let mut timers = vec![0, 0, 0];
    for event_id in sequence {
        let time = event_id / 3;
        let node = event_id % 3;
        assert_eq!(timers[node as usize], time);
        timers[node as usize] += 1;
    }
}

#[test]
fn test_dependency_resolver_pop() {
    let mut pending_events = PendingEvents::new();
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
            rev_id[pending_events.push(event)] = event_time * 3 + node_id;
        }
    }

    // remove 7 events such that every process had at least one timer fired
    // possible timer states after this:
    // - no timers
    // - one timer with delay 3
    // - two timers with delays 2 and 3
    for _ in 0..7 {
        let id = *pending_events
            .available_events()
            .iter()
            .choose(&mut rand::thread_rng())
            .unwrap();
        sequence.push(rev_id[id]);
        pending_events.pop(id);
    }

    // add one more timer to each process
    // if new timer delay is 3 or more it should be blocked by all other remaining timers if any
    // if new timer delay is less than 3, say 2.1, then it could "overtake" some of initial timers
    // (this may sound counter-intuitive since initial timers were set "at one moment" in this test,
    // however currently dependency resolver is implemented for general case when timers can be set
    // at different moments, while the optimization for timers set at one moment is not implemented)
    for node_id in 0..3 {
        let event = McEvent::TimerFired {
            proc: node_id.to_string(),
            timer: format!("{}", node_id),
            timer_delay: McTime::from(3.),
        };
        rev_id[pending_events.push(event)] = 9 + node_id;
    }
    while let Some(id) = pending_events.available_events().iter().choose(&mut rand::thread_rng()) {
        let id = *id;
        sequence.push(rev_id[id]);
        pending_events.pop(id);
    }
    println!("{:?}", sequence);
    assert_eq!(sequence.len(), 12);
    let mut timers = vec![0, 0, 0];
    for event_id in sequence {
        let time = event_id / 3;
        let node = event_id % 3;
        assert_eq!(timers[node as usize], time);
        timers[node as usize] += 1;
    }
}

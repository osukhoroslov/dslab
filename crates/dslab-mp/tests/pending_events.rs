use std::collections::BTreeSet;
use rand::prelude::IteratorRandom;
use rand::prelude::SliceRandom;

use dslab_mp::mc::events::{DeliveryOptions, McEvent, McEventId, McDuration};
use dslab_mp::mc::pending_events::PendingEvents;
use dslab_mp::message::Message;

#[test]
fn test_system_time() {
    let a = McDuration::from(0.0);
    let b = McDuration::from(0.0);
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
        let mut times: Vec<u64> = (0..3).into_iter().collect();
        times.shuffle(&mut rand::thread_rng());
        for event_time in times {
            let event = McEvent::TimerFired {
                proc: node_id.to_string(),
                timer: format!("{}", event_time),
                timer_delay: McDuration::from(event_time as f64),
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
        let mut times: Vec<u64> = (0..3).into_iter().collect();
        times.shuffle(&mut rand::thread_rng());
        for event_time in times {
            let event = McEvent::TimerFired {
                proc: node_id.to_string(),
                timer: format!("{}", event_time),
                timer_delay: McDuration::from(1.0 + event_time as f64),
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

    // this events would be last
    for node_id in 0..3 {
        let event = McEvent::TimerFired {
            proc: node_id.to_string(),
            timer: format!("{}", node_id),
            timer_delay: McDuration::from(2.1),
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

#[test]
fn test_timer_dependency_resolver_same_time() {
    let mut resolver = PendingEvents::new();
    let mut sequence = Vec::new();
    let mut rev_id = vec![0; 100];

    for node_id in 0..1 {
        let mut times: Vec<u64> = (0..100).into_iter().collect();
        times.shuffle(&mut rand::thread_rng());
        for event_time in times {
            let event = McEvent::TimerFired {
                proc: node_id.to_string(),
                timer: format!("{}", event_time),
                timer_delay: McDuration::from((event_time / 5) as f64),
            };
            rev_id[resolver.push(event)] = event_time;
        }
    }
    while let Some(id) = resolver.available_events().iter().choose(&mut rand::thread_rng()) {
        sequence.push(rev_id[*id]);
        resolver.pop(*id);
    }
    println!("{:?}", sequence);
    let mut timers = vec![0];
    for event_id in sequence {
        let time = event_id / 5;
        let node = 0;
        assert!(timers[node as usize] <= time);
        timers[node as usize] = time;
    }
}

#[test]
fn test_timer_dependency_resolver_stable_network() {
    let mut resolver = PendingEvents::new();
    let mut sequence = Vec::new();
    let times: Vec<u64> = (0..20).into_iter().collect();
    let mut rev_id = vec![0; 25];
    for event_time in times {
        let time = event_time.clamp(0, 11);
        if time == 10 {
            continue;
        }
        let event = McEvent::TimerFired {
            proc: "0".to_owned(),
            timer: format!("{}", event_time),
            timer_delay: McDuration::from(time as f64),
        };
        rev_id[resolver.push(event)] = event_time;
    }
    let message_times: Vec<u64> = (1..10).step_by(2).into_iter().collect();
    for message_time in message_times {
        let event = McEvent::MessageReceived {
            msg: Message {
                tip: "a".to_owned(),
                data: "hello".to_owned(),
            },
            src: "0".to_owned(),
            dest: "0".to_owned(),
            options: DeliveryOptions::NoFailures(McDuration::from(message_time as f64)),
        };
        rev_id[resolver.push(event)] = 20 + message_time / 2;
    }

    println!("{:?}", resolver.available_events());

    let count_timers_available =
        |available: &BTreeSet<McEventId>| available.iter().filter(|x| rev_id[**x] < 20).count();
    let count_messages_available =
        |available: &BTreeSet<McEventId>| available.len() - count_timers_available(available);

    assert!(count_timers_available(resolver.available_events()) == 1);
    assert!(count_messages_available(resolver.available_events()) == 5);
    while let Some(id) = resolver.available_events().iter().choose(&mut rand::thread_rng()) {
        let id = *id;
        println!("{}", rev_id[id]);
        sequence.push(id);
        resolver.pop(id);
        if count_timers_available(resolver.available_events()) > 1 {
            assert!(rev_id[id] == 9 || rev_id[id] == 24);
            break;
        }
        assert!(count_timers_available(resolver.available_events()) <= 1);
        assert!(count_messages_available(resolver.available_events()) <= 5);
    }
}

#[test]
fn test_timer_dependency_resolver_message_blocks_timer() {
    let mut resolver = PendingEvents::new();
    let mut sequence = Vec::new();
    let mut rev_id = vec![0; 25];

    for timer in 0..20 {
        let event = McEvent::TimerFired {
            proc: "0".to_owned(),
            timer: format!("{}", timer),
            timer_delay: McDuration::from(10.0 * (1.0 + (timer / 10) as f64)),
        };
        rev_id[resolver.push(event)] = timer;
    }
    let message = McEvent::MessageReceived {
        msg: Message {
            tip: "a".to_owned(),
            data: "hello".to_owned(),
        },
        src: "0".to_owned(),
        dest: "0".to_owned(),
        options: DeliveryOptions::NoFailures(McDuration::from(1.0)),
    };
    let message_id = resolver.push(message);
    rev_id[message_id] = 100;

    let count_timers_available =
        |available: &BTreeSet<McEventId>| available.iter().filter(|x| rev_id[**x] < 20).count();
    let count_messages_available =
        |available: &BTreeSet<McEventId>| available.len() - count_timers_available(available);

    assert!(count_timers_available(resolver.available_events()) == 0);
    assert!(count_messages_available(resolver.available_events()) == 1);
    resolver.pop(message_id);
    println!("{:?}", resolver.available_events());
    assert!(count_timers_available(resolver.available_events()) == 10);
    assert!(count_messages_available(resolver.available_events()) == 0);

    while let Some(id) = resolver.available_events().iter().choose(&mut rand::thread_rng()) {
        println!("{:?}", resolver.available_events());
        let id = *id;
        println!("{}", id);
        sequence.push(id);
        resolver.pop(id);
        assert!(count_timers_available(resolver.available_events()) <= 10);
    }
}

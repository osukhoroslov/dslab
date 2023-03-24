use std::cmp::Reverse;
use std::collections::{BinaryHeap, BTreeSet, HashMap, HashSet};

use rand::prelude::*;
use rand_pcg::Pcg64;

use crate::ls::common::*;

pub trait Neighborhood {
    fn step(&mut self, s: &State, instance: &Instance, rng: &mut Pcg64) -> State;
    /// resets the neighborhood before the next local search run
    fn reset(&mut self);
}

/// This is an implementation of a destroy-repair method for LNS
pub struct DestroyRepairNeighborhood {
    destroy_part: f64,
}

impl DestroyRepairNeighborhood {
    pub fn new(destroy_part: f64) -> Self {
        Self { destroy_part }
    }
}

pub fn sample(n: usize, k: usize, rng: &mut Pcg64) -> Vec<usize> {
    let mut s = BTreeSet::new();
    for i in 0..n {
        let var = rng.gen::<u64>();
        s.insert((var, i));
        if s.len() > k {
            let it = *s.iter().next_back().unwrap();
            s.remove(&it);
        }
    }
    let mut v = s.iter().map(|x| x.1).collect::<Vec<_>>();
    v.sort();
    v
}

impl Neighborhood for DestroyRepairNeighborhood {
    fn step(&mut self, s: &State, instance: &Instance, rng: &mut Pcg64) -> State {
        // destroy step: we remove |invocations| * destroy_part invocations from current state
        // possibly breaking some containers into several more containers
        // it can be proven that after breaking no constraints are violated
        let w = instance.keepalive;
        let cnt = ((instance.req_app.len() as f64) * self.destroy_part).ceil() as usize;
        //let del_vec = sample(instance.req_app.len(), cnt, rng);
        //let del = HashSet::<usize>::from_iter(del_vec.into_iter());
        /*let mut l = rng.gen_range(0..instance.req_app.len());
        let mut r = rng.gen_range(0..instance.req_app.len()+1);
        if l > r {
            let tmp = r;
            r = l;
            l = tmp;
        }
        let del = HashSet::<usize>::from_iter(l..r);
        let mut assignment = HashMap::new();
        for container in s.containers.iter() {
            for id in container.invocations.iter().copied() {
                if !del.contains(&id) {
                    assignment.insert(id, container.host);
                }
            }
        }*/
        let del_hosts = HashSet::<usize>::from_iter(sample(instance.hosts.len(), 1, rng).into_iter());
        let mut assignment = HashMap::new();
        for container in s.containers.iter() {
            for id in container.invocations.iter().copied() {
                if !del_hosts.contains(&container.host) {
                    assignment.insert(id, container.host);
                }
            }
        }
        let mut conts = Vec::<Container>::new();
        let mut used = vec![vec![0u64; instance.hosts[0].len()]; instance.hosts.len()];
        let mut containers_by_host = vec![HashSet::<usize>::new(); instance.hosts.len()];
        let mut expire = BTreeSet::<(u64, usize)>::new();
        let mut queue = BinaryHeap::with_capacity(instance.req_start.len());
        for (id, t) in instance.req_start.iter().enumerate() {
            queue.push((Reverse(*t), id));
        }
        while let Some((Reverse(time), i)) = queue.pop() {
            while let Some(x) = expire.first() {
                if x.0 > time {
                    break;
                }
                let evt = expire.pop_first().unwrap();
                let c = evt.1;
                for r in 0..used[conts[c].host].len() {
                    used[conts[c].host][r] -= conts[c].resources[r];
                }
                containers_by_host[conts[c].host].remove(&c);
            }
            let mut host = usize::MAX;
            if let Some(h) = assignment.get(&i) {
                host = *h;
            } else {
                let mut status = i64::MIN;
                let mut possible = 0..used.len();//sample(used.len(), used.len()/2, rng);
                for h in possible {
                    /*if !del_hosts.contains(&h) {
                        continue;
                    }*/
                    for c in containers_by_host[h].iter().copied() {
                        if conts[c].app == instance.req_app[i] && time >= conts[c].end - w {
                            host = h;
                            status = 3;
                            break;
                        }
                    }
                    if status == 3 {
                        break;
                    }
                    let mut exceed = false;
                    for r in 0..used[h].len() {
                        if used[h][r] + instance.apps[instance.req_app[i]][r] > instance.hosts[h][r] {
                            exceed = true;
                            break;
                        }
                    }
                    if !exceed {
                        status = 2;
                        host = h;
                        continue;
                    }
                    let curr = -(containers_by_host[h].len() as i64);
                    if curr > status {
                        status = curr;
                        host = h;
                    }
                }
            }
            let mut placed = false;
            for c in containers_by_host[host].iter().copied() {
                if conts[c].app == instance.req_app[i] && time >= conts[c].end - w {
                    expire.remove(&(conts[c].end, c));
                    conts[c].end = time.min(conts[c].end - w).max(instance.req_start[i]) + instance.req_dur[i] + w;
                    expire.insert((conts[c].end, c));
                    conts[c].invocations.push(i);
                    placed = true;
                    break;
                }
            }
            if placed {
                continue;
            }
            placed = true;
            for r in 0..used[host].len() {
                if used[host][r] + instance.apps[instance.req_app[i]][r] > instance.hosts[host][r] {
                    placed = false;
                    break;
                }
            }
            if placed {
                let new = Container {
                    host,
                    app: instance.req_app[i],
                    invocations: vec![i],
                    resources: instance.apps[instance.req_app[i]].clone(),
                    start: time,
                    end: time + instance.req_dur[i] + w + instance.app_coldstart[instance.req_app[i]],
                };
                expire.insert((new.end, conts.len()));
                containers_by_host[host].insert(conts.len());
                for r in 0..used[host].len() {
                    used[host][r] += instance.apps[instance.req_app[i]][r];
                }
                conts.push(new);
                continue;
            }
            let mut best_time = u64::MAX;
            let mut curr = used[host].clone();
            for (t, c) in expire.iter().copied() {
                if conts[c].host == host {
                    let mut ok = true;
                    for r in 0..curr.len() {
                        curr[r] -= conts[c].resources[r];
                        if curr[r] + instance.apps[instance.req_app[i]][r] > instance.hosts[host][r] {
                            ok = false;
                        }
                    }
                    if ok {
                        best_time = t;
                        break;
                    }
                }
            }
            queue.push((Reverse(best_time), i));
            assignment.insert(i, host);
        }
        let mut nxt = State {
            containers: conts,
            objective: 0,
        };
        nxt.recompute_objective(instance);
        println!("nxt obj = {}", nxt.objective);
        match nxt.validate(instance) {
            Ok(_) => {}
            Err(e) => {
                panic!("{}", e);
            }
        }
        nxt
    }

    fn reset(&mut self) {}
}



pub struct TestNeighborhood {
    destroy_part: f64,
}

impl TestNeighborhood {
    pub fn new(destroy_part: f64) -> Self {
        Self { destroy_part }
    }
}


impl Neighborhood for TestNeighborhood {
    fn step(&mut self, s: &State, instance: &Instance, rng: &mut Pcg64) -> State {
        // destroy step: we remove |invocations| * destroy_part invocations from current state
        // possibly breaking some containers into several more containers
        // it can be proven that after breaking no constraints are violated
        let w = instance.keepalive;
        let cnt = ((instance.req_app.len() as f64) * self.destroy_part).ceil() as usize;
        //let del_vec = sample(instance.req_app.len(), cnt, rng);
        //let del = HashSet::<usize>::from_iter(del_vec.into_iter());
        /*let mut l = rng.gen_range(0..instance.req_app.len());
        let mut r = rng.gen_range(0..instance.req_app.len()+1);
        if l > r {
            let tmp = r;
            r = l;
            l = tmp;
        }
        let del = HashSet::<usize>::from_iter(l..r);
        let mut assignment = HashMap::new();
        for container in s.containers.iter() {
            for id in container.invocations.iter().copied() {
                if !del.contains(&id) {
                    assignment.insert(id, container.host);
                }
            }
        }*/
        let del_hosts = HashSet::<usize>::from_iter(sample(instance.hosts.len(), 1, rng).into_iter());
        let mut assignment = HashMap::new();
        for container in s.containers.iter() {
            for id in container.invocations.iter().copied() {
                if !del_hosts.contains(&container.host) {
                    assignment.insert(id, container.host);
                }
            }
        }
        let mut conts = Vec::<Container>::new();
        let mut used = vec![vec![0u64; instance.hosts[0].len()]; instance.hosts.len()];
        let mut containers_by_host = vec![HashSet::<usize>::new(); instance.hosts.len()];
        let mut expire = BTreeSet::<(u64, usize)>::new();
        let mut queue = BinaryHeap::with_capacity(instance.req_start.len());
        for (id, t) in instance.req_start.iter().enumerate() {
            queue.push((Reverse(*t), id));
        }
        while let Some((Reverse(time), i)) = queue.pop() {
            while let Some(x) = expire.first() {
                if x.0 > time {
                    break;
                }
                let evt = expire.pop_first().unwrap();
                let c = evt.1;
                for r in 0..used[conts[c].host].len() {
                    used[conts[c].host][r] -= conts[c].resources[r];
                }
                containers_by_host[conts[c].host].remove(&c);
            }
            let mut host = usize::MAX;
            if let Some(h) = assignment.get(&i) {
                host = *h;
            } else {
                let mut status = i64::MIN;
                let mut possible = 0..used.len();//sample(used.len(), used.len()/2, rng);
                for h in possible {
                    /*if !del_hosts.contains(&h) {
                        continue;
                    }*/
                    for c in containers_by_host[h].iter().copied() {
                        if conts[c].app == instance.req_app[i] && time >= conts[c].end - w {
                            host = h;
                            status = 3;
                            break;
                        }
                    }
                    if status == 3 {
                        break;
                    }
                    let mut exceed = false;
                    for r in 0..used[h].len() {
                        if used[h][r] + instance.apps[instance.req_app[i]][r] > instance.hosts[h][r] {
                            exceed = true;
                            break;
                        }
                    }
                    if !exceed {
                        status = 2;
                        host = h;
                        continue;
                    }
                    let curr = -(containers_by_host[h].len() as i64);
                    if curr > status {
                        status = curr;
                        host = h;
                    }
                }
            }
            let mut placed = false;
            for c in containers_by_host[host].iter().copied() {
                if conts[c].app == instance.req_app[i] && time >= conts[c].end - w {
                    expire.remove(&(conts[c].end, c));
                    conts[c].end = time.min(conts[c].end - w).max(instance.req_start[i]) + instance.req_dur[i] + w;
                    expire.insert((conts[c].end, c));
                    conts[c].invocations.push(i);
                    placed = true;
                    break;
                }
            }
            if placed {
                continue;
            }
            placed = true;
            for r in 0..used[host].len() {
                if used[host][r] + instance.apps[instance.req_app[i]][r] > instance.hosts[host][r] {
                    placed = false;
                    break;
                }
            }
            if placed {
                let new = Container {
                    host,
                    app: instance.req_app[i],
                    invocations: vec![i],
                    resources: instance.apps[instance.req_app[i]].clone(),
                    start: time,
                    end: time + instance.req_dur[i] + w + instance.app_coldstart[instance.req_app[i]],
                };
                expire.insert((new.end, conts.len()));
                containers_by_host[host].insert(conts.len());
                for r in 0..used[host].len() {
                    used[host][r] += instance.apps[instance.req_app[i]][r];
                }
                conts.push(new);
                continue;
            }
            let mut best_time = u64::MAX;
            let mut curr = used[host].clone();
            for (t, c) in expire.iter().copied() {
                if conts[c].host == host {
                    let mut ok = true;
                    for r in 0..curr.len() {
                        curr[r] -= conts[c].resources[r];
                        if curr[r] + instance.apps[instance.req_app[i]][r] > instance.hosts[host][r] {
                            ok = false;
                        }
                    }
                    if ok {
                        best_time = t;
                        break;
                    }
                }
            }
            queue.push((Reverse(best_time), i));
            assignment.insert(i, host);
        }
        let mut nxt = State {
            containers: conts,
            objective: 0,
        };
        nxt.recompute_objective(instance);
        println!("nxt obj = {}", nxt.objective);
        match nxt.validate(instance) {
            Ok(_) => {}
            Err(e) => {
                panic!("{}", e);
            }
        }
        nxt
    }

    fn reset(&mut self) {}
}

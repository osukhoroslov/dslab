use crate::util::Counter;

use std::collections::{HashMap, HashSet};

pub struct Host {
    pub id: u64,
    containers: HashSet<u64>,
}

impl Host {
    pub fn new(id: u64) -> Self {
        Self {
            id,
            containers: Default::default(),
        }
    }

    pub fn new_container(&mut self, id: u64) {
        self.containers.insert(id);
    }

    pub fn delete_container(&mut self, id: u64) {
        self.containers.remove(&id);
    }
}

#[derive(Default)]
pub struct HostManager {
    host_ctr: Counter,
    hosts: HashMap<u64, Host>,
}

impl HostManager {
    pub fn get_host(&self, id: u64) -> Option<&Host> {
        self.hosts.get(&id)
    }

    pub fn get_host_mut(&mut self, id: u64) -> Option<&mut Host> {
        self.hosts.get_mut(&id)
    }

    pub fn get_hosts(&self) -> HostIterator<'_> {
        HostIterator { inner: self.hosts.iter() }
    }

    pub fn get_hosts_mut(&mut self) -> HostIteratorMut<'_> {
        HostIteratorMut { inner: self.hosts.iter_mut() }
    }

    pub fn new_host(&mut self) -> u64 {
        let id = self.host_ctr.next();
        let host = Host::new(id);
        self.hosts.insert(id, host);
        id
    }
}

pub struct HostIterator<'a> {
    inner: std::collections::hash_map::Iter<'a, u64, Host>,
}

impl<'a> Iterator for HostIterator<'a> {
    type Item = &'a Host;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some((_, v)) = self.inner.next() {
            Some(v)
        } else {
            None
        }
    }
}

pub struct HostIteratorMut<'a> {
    inner: std::collections::hash_map::IterMut<'a, u64, Host>,
}

impl<'a> Iterator for HostIteratorMut<'a> {
    type Item = &'a mut Host;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some((_, v)) = self.inner.next() {
            Some(v)
        } else {
            None
        }
    }
}

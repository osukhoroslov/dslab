use core::actor::{ActorId};

use std::collections::HashMap;

extern crate env_logger;

pub struct File {
    disk_actor_id: ActorId,
    size: u64,
    position: u64,
    opened: bool,
}

impl File {
    pub fn new(disk_actor_id: ActorId, size: u64) -> File {
        File {
            disk_actor_id,
            size,
            position: 0,
            opened: false,
        }
    }

    pub fn seek(&mut self, position: u64) -> Option<u64> {
        if position > self.size {
            return None;
        }
        self.position = position;
        Some(self.position)
    }

    pub fn close(&mut self) {
        self.opened = false;
    }
}

pub struct FileSystem {
    files: HashMap<String, File>,
    disk_actor_id: ActorId,
}

impl FileSystem {
    pub fn new(disk_actor_id: ActorId) -> Self {
        Self {
            files: HashMap::new(),
            disk_actor_id,
        }
    }

    pub fn open(&mut self, name: &str) -> &mut File {
        let file = self
            .files
            .entry(name.to_string())
            .or_insert(File::new(self.disk_actor_id.clone(), 0));
        file.opened = true;
        file
    }
}

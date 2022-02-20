use std::{borrow::Borrow, collections::HashMap};

use core::{
    actor::{Actor, ActorContext, ActorId, Event},
    match_event,
    sim::Simulation,
};

use crate::api::*;

pub const FS_ID: &str = "fs";

pub struct File {
    size: u64,
}

impl File {
    pub fn new(size: u64) -> File {
        File { size }
    }
}

pub struct FileSystem {
    files: HashMap<String, File>,
    disk_actor_id: ActorId,
    opened_files: HashMap<FD, String>,
    requests: HashMap<u64, (ActorId, FD)>,
    max_used_fd: u64,
}

impl FileSystem {
    pub fn new(disk_actor_id: ActorId) -> Self {
        Self {
            files: HashMap::new(),
            disk_actor_id,
            opened_files: HashMap::new(),
            requests: HashMap::new(),
            max_used_fd: 0,
        }
    }

    pub fn open(&mut self, name: &str) -> FD {
        self.files.entry(name.to_string()).or_insert(File::new(0));
        self.max_used_fd += 1;
        self.opened_files.insert(self.max_used_fd, name.to_string());
        self.max_used_fd
    }

    pub fn read_async(&mut self, fd: FD, size: u64, sim: &mut Simulation, actor_to_notify: ActorId) {
        sim.add_event_now(FileReadRequest { fd, size }, actor_to_notify, ActorId::from(FS_ID));
    }

    pub fn write_async(&mut self, fd: FD, size: u64, sim: &mut Simulation, actor_to_notify: ActorId) {
        sim.add_event_now(FileWriteRequest { fd, size }, actor_to_notify, ActorId::from(FS_ID));
    }

    pub fn close(&mut self, fd: FD) {
        self.opened_files.remove(fd.borrow());
    }
}

impl Actor for FileSystem {
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext) {
        match_event!( event {
            &FileReadRequest { fd, size } => {
                let event_id = ctx.emit_now(DataReadRequest { size }, self.disk_actor_id.clone());
                self.requests.insert(
                    event_id,
                    (from.clone(), fd)
                );

                let path = self.opened_files[&fd].clone();
                println!("{} [{}] requested READ {} bytes from file {} (fd {})", ctx.time(), ctx.id, size, path, fd);
            },
            &FileWriteRequest { fd, size } => {
                let event_id = ctx.emit_now(DataWriteRequest { size }, self.disk_actor_id.clone());
                self.requests.insert(
                    event_id,
                    (from.clone(), fd)
                );

                let path = self.opened_files[&fd].clone();
                println!("{} [{}] requested WRITE {} bytes to file {} (fd {})", ctx.time(), ctx.id, size, path, fd);
            },
            &DataReadCompleted { src_event_id, size } => {
                match self.requests.get(&src_event_id) {
                    Some((actor_to_notify, fd)) => {
                        let path = self.opened_files[&fd].clone();

                        println!("{} [{}] completed READ {} bytes from {} (fd {})", ctx.time(), ctx.id, size, path, fd);

                        ctx.emit_now(FileReadCompleted { fd: fd.clone() }, actor_to_notify.clone());

                        self.requests.remove(&src_event_id);
                    },
                    None => {
                        panic!("request not found, unexpected");
                    }
                }
            },
            &DataWriteCompleted { src_event_id, size } => {
                match self.requests.get(&src_event_id) {
                    Some((actor_to_notify, fd)) => {
                        let file = &self.files[&self.opened_files[&fd]];
                        let path = self.opened_files[&fd].clone();

                        println!("{} [{}] completed WRITE {} bytes to {} (fd {})", ctx.time(), ctx.id, size, path, fd);

                        ctx.emit_now(FileWriteCompleted { fd: fd.clone(), new_size: file.size }, actor_to_notify.clone());

                        self.files.entry(path).and_modify(|file| { file.size += size });
                        self.requests.remove(&src_event_id);
                    },
                    None => {
                        panic!("request not found, unexpected");
                    }
                }
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}

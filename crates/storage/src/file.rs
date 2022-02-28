use std::collections::HashMap;

use core::{
    actor::{Actor, ActorContext, ActorId, Event},
    match_event,
};

use crate::api::*;

struct File {
    size: u64,
}

impl File {
    fn new(size: u64) -> File {
        File { size }
    }
}

pub struct FileSystem {
    name: String,
    files: HashMap<String, File>,
    disk_actor_id: ActorId,
    requests: HashMap<u64, (ActorId, String)>,
}

impl FileSystem {
    pub fn new(name: &str, disk_actor_id: ActorId) -> Self {
        Self {
            name: name.to_string(),
            files: HashMap::new(),
            disk_actor_id,
            requests: HashMap::new(),
        }
    }

    pub fn create(&mut self, name: &str) -> bool {
        if let Some(_) = self.files.get(name) {
            false
        } else {
            self.files.insert(name.to_string(), File::new(0));
            true
        }
    }

    pub fn get_size(&mut self, name: &str) -> Option<u64> {
        if let Some(file) = self.files.get(name) {
            Some(file.size)
        } else {
            None
        }
    }

    pub fn read(&mut self, file_name: &str, size: u64, ctx: &mut ActorContext) -> u64 {
        ctx.emit_now(
            FileReadRequest {
                file_name: file_name.to_string(),
                size: Some(size),
            },
            ActorId::from(&self.name),
        )
    }

    pub fn read_all(&mut self, file_name: &str, ctx: &mut ActorContext) -> u64 {
        ctx.emit_now(
            FileReadRequest {
                file_name: file_name.to_string(),
                size: None,
            },
            ActorId::from(&self.name),
        )
    }

    pub fn write(&mut self, file_name: &str, size: u64, ctx: &mut ActorContext) -> u64 {
        ctx.emit_now(
            FileWriteRequest {
                file_name: file_name.to_string(),
                size: size,
            },
            ActorId::from(&self.name),
        )
    }

    pub fn delete(&mut self, name: &str) {
        self.files.remove(name);
    }
}

impl Actor for FileSystem {
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext) {
        match_event!( event {
            FileReadRequest { file_name , size } => {
                if let Some(file) = self.files.get(file_name) {
                    let size_to_read = if let Some(value) = size {
                        file.size.min(*value)
                    } else {
                        file.size
                    };

                    let event_id = ctx.emit_now(DataReadRequest { size: size_to_read }, self.disk_actor_id.clone());

                    self.requests.insert(
                        event_id,
                        (from.clone(), file_name.clone())
                    );

                    println!("{} [{}] requested READ {} bytes from file {}", ctx.time(), ctx.id, size_to_read, file_name);
                } else {
                    panic!("File not created!");
                }
            },
            FileWriteRequest { file_name, size } => {
                if let Some(_) = self.files.get(file_name) {
                    let event_id = ctx.emit_now(DataWriteRequest { size: *size }, self.disk_actor_id.clone());

                    self.requests.insert(
                        event_id,
                        (from.clone(), file_name.clone())
                    );

                    println!("{} [{}] requested WRITE {} bytes to file {}", ctx.time(), ctx.id, size, file_name);
                } else {
                    panic!("File not created!");
                }
            },
            &DataReadCompleted { src_event_id, size } => {
                if let Some((requester, file_name)) = self.requests.get(&src_event_id) {
                    if let Some(_) = self.files.get(file_name) {
                        println!("{} [{}] completed READ {} bytes from {}", ctx.time(), ctx.id, size, file_name);
                        ctx.emit_now(FileReadCompleted { file_name: file_name.clone(), read_size: size }, requester.clone());
                        self.requests.remove(&src_event_id);
                    } else {
                        panic!("Request not found!");
                    }
                }
            },
            &DataWriteCompleted { src_event_id, size } => {
                if let Some((requester, file_name)) = self.requests.get(&src_event_id) {
                    if let Some(file) = self.files.get_mut(file_name) {
                        file.size += size;
                        println!("{} [{}] completed WRITE {} bytes to {}, new size {}", ctx.time(), ctx.id, size, file_name, file.size);
                        ctx.emit_now(FileWriteCompleted { file_name: file_name.clone(), new_size: file.size }, requester.clone());
                        self.requests.remove(&src_event_id);
                    } else {
                        panic!("Request not found!");
                    }
                }
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}

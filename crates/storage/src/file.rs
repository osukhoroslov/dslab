use std::{cell::RefCell, collections::HashMap, rc::Rc};

use core::{cast, context::SimulationContext, event::Event, handler::EventHandler};

use crate::{api::*, disk::Disk};

struct File {
    size: u64,
}

impl File {
    fn new(size: u64) -> File {
        File { size }
    }
}

pub struct FileSystem {
    ctx: SimulationContext,
    files: HashMap<String, File>,
    disks: HashMap<String, Rc<RefCell<Disk>>>,
    requests: HashMap<u64, (String, String)>, // event_id -> (component_id, file_name)
}

impl FileSystem {
    pub fn new(ctx: SimulationContext) -> Self {
        Self {
            ctx,
            files: HashMap::new(),
            disks: HashMap::new(),
            requests: HashMap::new(),
        }
    }

    pub fn mount_disk(&mut self, mount_point: &str, disk: Rc<RefCell<Disk>>) -> bool {
        self.disks.insert(mount_point.to_string(), disk).is_some()
    }

    pub fn unmount_disk(&mut self, mount_point: &str) -> bool {
        self.disks.remove(mount_point).is_some()
    }

    fn resolve_disk(&self, file_name: &str) -> Option<Rc<RefCell<Disk>>> {
        for (mount_point, disk) in &self.disks {
            if file_name.starts_with(mount_point) {
                return Some(disk.clone());
            }
        }
        None
    }

    pub fn create_file(&mut self, file_name: &str) -> bool {
        if self.files.contains_key(file_name) {
            return false;
        } else if self.resolve_disk(file_name).is_some() {
            self.files.insert(file_name.to_string(), File::new(0));
            return true;
        }
        false
    }

    pub fn get_file_size(&self, file_name: &str) -> Option<u64> {
        self.files.get(file_name).map(|f| f.size)
    }

    pub fn get_used_space(&self) -> u64 {
        self.disks.iter().map(|(_, v)| v.borrow().get_used_space()).sum()
    }

    pub fn delete_file(&mut self, name: &str) {
        self.files.remove(name);
    }
}

impl EventHandler for FileSystem {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            FileReadRequest { file_name, size } => {
                if let Some(file) = self.files.get(&file_name) {
                    let size_to_read = if let Some(value) = size {
                        file.size.min(value)
                    } else {
                        file.size
                    };

                    if let Some(disk) = self.resolve_disk(&file_name) {
                        let event_id = self
                            .ctx
                            .emit_now(DataReadRequest { size: size_to_read }, disk.borrow().id());

                        self.requests.insert(event_id, (event.src.clone(), file_name.clone()));

                        println!(
                            "{} [{}] requested READ {} bytes from file {}",
                            self.ctx.time(),
                            self.ctx.id(),
                            size_to_read,
                            file_name
                        );
                    } else {
                        panic!("Cannot resolve disk");
                    }
                } else {
                    panic!("File not created!");
                }
            }
            FileWriteRequest { file_name, size } => {
                if self.files.contains_key(&file_name) {
                    if let Some(disk) = self.resolve_disk(&file_name) {
                        let event_id = self.ctx.emit_now(DataWriteRequest { size }, disk.borrow().id());

                        self.requests.insert(event_id, (event.src.clone(), file_name.clone()));

                        println!(
                            "{} [{}] requested WRITE {} bytes to file {}",
                            self.ctx.time(),
                            self.ctx.id(),
                            size,
                            file_name
                        );
                    } else {
                        panic!("Cannot resolve disk");
                    }
                } else {
                    panic!("File not created!");
                }
            }
            DataReadCompleted { src_event_id, size } => {
                if let Some((requester, file_name)) = self.requests.get(&src_event_id) {
                    if self.files.contains_key(file_name) {
                        println!(
                            "{} [{}] completed READ {} bytes from {}",
                            self.ctx.time(),
                            self.ctx.id(),
                            size,
                            file_name
                        );
                        self.ctx.emit_now(
                            FileReadCompleted {
                                file_name: file_name.clone(),
                                read_size: size,
                            },
                            requester.clone(),
                        );
                        self.requests.remove(&src_event_id);
                    } else {
                        panic!("Request not found!");
                    }
                }
            }
            DataWriteCompleted { src_event_id, size } => {
                if let Some((requester, file_name)) = self.requests.get(&src_event_id) {
                    if let Some(file) = self.files.get_mut(file_name) {
                        file.size += size;
                        println!(
                            "{} [{}] completed WRITE {} bytes to {}, new size {}",
                            self.ctx.time(),
                            self.ctx.id(),
                            size,
                            file_name,
                            file.size
                        );
                        self.ctx.emit_now(
                            FileWriteCompleted {
                                file_name: file_name.clone(),
                                new_size: file.size,
                            },
                            requester.clone(),
                        );
                        self.requests.remove(&src_event_id);
                    } else {
                        panic!("Request not found!");
                    }
                }
            }
        })
    }
}

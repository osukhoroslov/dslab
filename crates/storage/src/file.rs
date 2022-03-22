use std::{cell::RefCell, collections::HashMap, rc::Rc};

use core::{cast, context::SimulationContext, event::Event, handler::EventHandler, log_debug};

use crate::{api::*, disk::Disk};

struct File {
    size: u64,
    cnt_actions: u64, // number of timed actions on this file. File can be removed only if there are no actions on it
}

impl File {
    fn new(size: u64) -> File {
        File { size, cnt_actions: 0 }
    }
}

pub struct FileSystem {
    files: HashMap<String, File>,
    disks: HashMap<String, Rc<RefCell<Disk>>>,
    requests: HashMap<(String, u64), (u64, String, String)>, // (disk id, disk_request_id) -> (request_id, requester, file_name)
    next_request_id: u64,
    ctx: SimulationContext,
}

impl FileSystem {
    pub fn new(ctx: SimulationContext) -> Self {
        Self {
            files: HashMap::new(),
            disks: HashMap::new(),
            requests: HashMap::new(),
            next_request_id: 0,
            ctx,
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

    fn get_unique_request_id(&mut self) -> u64 {
        let request_id = self.next_request_id;
        self.next_request_id += 1;
        request_id
    }

    pub fn read<S: Into<String>>(&mut self, file_name: &str, size: u64, requester: S) -> u64 {
        self.read_impl(file_name, Some(size), requester)
    }

    pub fn read_all<S: Into<String>>(&mut self, file_name: &str, requester: S) -> u64 {
        self.read_impl(file_name, None, requester)
    }

    fn read_impl<S: Into<String>>(&mut self, file_name: &str, size: Option<u64>, requester: S) -> u64 {
        let request_id = self.get_unique_request_id();

        if let Some(disk) = self.resolve_disk(&file_name) {
            if let Some(file) = self.files.get_mut(file_name) {
                let size_to_read = if let Some(value) = size {
                    if file.size < value {
                        self.ctx.emit_now(
                            FileReadFailed {
                                request_id,
                                file_name: file_name.to_string(),
                                error: "too large size requested".to_string(),
                            },
                            requester,
                        );

                        return request_id;
                    }
                    value
                } else {
                    file.size
                };

                log_debug!(
                    self.ctx,
                    "Requested read {} bytes from file [{}]",
                    size_to_read,
                    file_name,
                );

                file.cnt_actions += 1;
                let disk_request_id = disk.borrow_mut().read(size_to_read, self.ctx.id());
                self.requests.insert(
                    (disk.borrow_mut().id().to_string(), disk_request_id),
                    (request_id, requester.into(), file_name.into()),
                );
            } else {
                self.ctx.emit_now(
                    FileReadFailed {
                        request_id,
                        file_name: file_name.to_string(),
                        error: "file does not exist".to_string(),
                    },
                    requester,
                );
            }
        } else {
            self.ctx.emit_now(
                FileReadFailed {
                    request_id,
                    file_name: file_name.to_string(),
                    error: "cannot resolve disk".to_string(),
                },
                requester,
            );
        }

        request_id
    }

    pub fn write<S: Into<String>>(&mut self, file_name: &str, size: u64, requester: S) -> u64 {
        let request_id = self.get_unique_request_id();

        if let Some(disk) = self.resolve_disk(file_name) {
            if let Some(file) = self.files.get_mut(file_name) {
                log_debug!(self.ctx, "Requested write {} bytes to file [{}]", size, file_name,);

                file.cnt_actions += 1;
                let disk_request_id = disk.borrow_mut().write(size, self.ctx.id());
                self.requests.insert(
                    (disk.borrow_mut().id().to_string(), disk_request_id),
                    (request_id, requester.into(), file_name.into()),
                );
            } else {
                self.ctx.emit_now(
                    FileWriteFailed {
                        request_id,
                        file_name: file_name.to_string(),
                        error: "file does not exist".to_string(),
                    },
                    requester,
                );
            }
        } else {
            self.ctx.emit_now(
                FileWriteFailed {
                    request_id,
                    file_name: file_name.to_string(),
                    error: "cannot resolve disk".to_string(),
                },
                requester,
            );
        }

        request_id
    }

    pub fn create_file(&mut self, file_name: &str) -> bool {
        log_debug!(self.ctx, "Requested to create file [{}]", file_name);
        if self.files.contains_key(file_name) {
            log_debug!(self.ctx, "File already exists");
            return false;
        } else if let Some(disk) = self.resolve_disk(file_name) {
            log_debug!(
                self.ctx,
                "File [{}] created on disk [{}]",
                file_name,
                disk.borrow_mut().id()
            );

            self.files.insert(file_name.to_string(), File::new(0));
            return true;
        }
        log_debug!(self.ctx, "Cannot resolve mount point for path [{}]", file_name);
        false
    }

    pub fn get_file_size(&self, file_name: &str) -> Option<u64> {
        self.files.get(file_name).map(|f| f.size)
    }

    pub fn get_used_space(&self) -> u64 {
        self.disks.iter().map(|(_, v)| v.borrow().get_used_space()).sum()
    }

    pub fn delete_file(&mut self, file_name: &str) -> bool {
        if let Some(disk) = self.resolve_disk(file_name) {
            if let Some(file) = self.files.get(file_name) {
                if file.cnt_actions == 0 {
                    log_debug!(self.ctx, "Removing file [{}]", file_name);
                    disk.borrow_mut().mark_free(file.size);
                    self.files.remove(file_name);
                    return true;
                }
                log_debug!(self.ctx, "File [{}] is busy and cannot be removed", file_name);
            }
        }
        false
    }
}

impl EventHandler for FileSystem {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            DataReadCompleted {
                request_id: disk_request_id,
                size,
            } => {
                let key = (event.src, disk_request_id);
                if let Some((request_id, requester, file_name)) = self.requests.get(&key) {
                    if let Some(file) = self.files.get_mut(file_name) {
                        log_debug!(self.ctx, "Completed reading {} bytes from file [{}]", size, file_name);
                        file.cnt_actions -= 1;
                        self.ctx.emit_now(
                            FileReadCompleted {
                                request_id: *request_id,
                                file_name: file_name.clone(),
                                read_size: size,
                            },
                            requester.clone(),
                        );
                        self.requests.remove(&key);
                    } else {
                        panic!("File [{}] was lost while reading", file_name);
                    }
                } else {
                    panic!("Request not found");
                }
            }
            DataReadFailed {
                request_id: disk_request_id,
                error,
            } => {
                let key = (event.src, disk_request_id);
                if let Some((request_id, requester, file_name)) = self.requests.get(&key) {
                    if let Some(file) = self.files.get_mut(file_name) {
                        log_debug!(self.ctx, "Failed reading from file [{}], error: {}", file_name, error);
                        file.cnt_actions -= 1;
                        self.ctx.emit_now(
                            FileReadFailed {
                                request_id: *request_id,
                                file_name: file_name.clone(),
                                error,
                            },
                            requester.clone(),
                        );
                        self.requests.remove(&key);
                    } else {
                        panic!("File [{}] was lost while reading", file_name);
                    }
                } else {
                    panic!("Request not found");
                }
            }
            DataWriteCompleted {
                request_id: disk_request_id,
                size,
            } => {
                let key = (event.src, disk_request_id);
                if let Some((request_id, requester, file_name)) = self.requests.get(&key) {
                    if let Some(file) = self.files.get_mut(file_name) {
                        file.size += size;
                        file.cnt_actions -= 1;
                        log_debug!(
                            self.ctx,
                            "Completed writing {} bytes to file [{}], new size {}",
                            size,
                            file_name,
                            file.size,
                        );
                        self.ctx.emit_now(
                            FileWriteCompleted {
                                request_id: *request_id,
                                file_name: file_name.clone(),
                                new_size: file.size,
                            },
                            requester.clone(),
                        );
                        self.requests.remove(&key);
                    } else {
                        panic!("File [{}] was lost while writing", file_name);
                    }
                } else {
                    panic!("Request not found");
                }
            }
            DataWriteFailed {
                request_id: disk_request_id,
                error,
            } => {
                let key = (event.src, disk_request_id);
                if let Some((request_id, requester, file_name)) = self.requests.get(&key) {
                    if let Some(file) = self.files.get_mut(file_name) {
                        file.cnt_actions -= 1;
                        log_debug!(self.ctx, "Failed writing to file [{}], error: {}", file_name, error,);
                        self.ctx.emit_now(
                            FileWriteFailed {
                                request_id: *request_id,
                                file_name: file_name.clone(),
                                error,
                            },
                            requester.clone(),
                        );
                        self.requests.remove(&key);
                    } else {
                        panic!("File [{}] was lost while writing", file_name);
                    }
                } else {
                    panic!("Request not found");
                }
            }
        })
    }
}

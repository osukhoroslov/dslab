use std::{cell::RefCell, collections::HashMap, rc::Rc};

use core::component::Id;
use core::{cast, context::SimulationContext, event::Event, handler::EventHandler, log_debug, log_error};

use crate::{disk::Disk, events::*};

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
    requests: HashMap<(Id, u64), (u64, Id, String)>, // (disk id, disk_request_id) -> (request_id, requester, file_name)
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

    pub fn mount_disk(&mut self, mount_point: &str, disk: Rc<RefCell<Disk>>) -> Result<(), String> {
        log_debug!(self.ctx, "Received mount disk request, mount_point: [{}]", mount_point);
        if let Some(_) = self.disks.get(mount_point) {
            return Err(format!("mount point [{}] is already is use", mount_point));
        }
        self.disks.insert(mount_point.to_string(), disk);
        Ok(())
    }

    pub fn unmount_disk(&mut self, mount_point: &str) -> Result<(), String> {
        log_debug!(
            self.ctx,
            "Received unmount disk request, mount_point: [{}]",
            mount_point
        );
        if let None = self.disks.remove(mount_point) {
            return Err(format!("unknown mount point [{}]", mount_point));
        }
        Ok(())
    }

    fn resolve_disk(&self, file_name: &str) -> Result<Rc<RefCell<Disk>>, String> {
        for (mount_point, disk) in &self.disks {
            if file_name.starts_with(mount_point) {
                return Ok(disk.clone());
            }
        }
        Err(format!("cannot resolve on which disk file [{}] is located", file_name))
    }

    fn get_unique_request_id(&mut self) -> u64 {
        let request_id = self.next_request_id;
        self.next_request_id += 1;
        request_id
    }

    pub fn read(&mut self, file_name: &str, size: u64, requester: Id) -> u64 {
        log_debug!(
            self.ctx,
            "Received read request, size: {}, file: [{}], requester: {}",
            size,
            file_name,
            requester
        );
        self.read_impl(file_name, Some(size), requester)
    }

    pub fn read_all(&mut self, file_name: &str, requester: Id) -> u64 {
        log_debug!(
            self.ctx,
            "Received read request, size: all, file: [{}], requester: {}",
            file_name,
            requester
        );
        self.read_impl(file_name, None, requester)
    }

    fn read_impl(&mut self, file_name: &str, size: Option<u64>, requester: Id) -> u64 {
        let request_id = self.get_unique_request_id();
        match self.resolve_disk(&file_name) {
            Ok(disk) => {
                if let Some(file) = self.files.get_mut(file_name) {
                    let size_to_read = if let Some(value) = size {
                        if file.size < value {
                            let error = format!("requested read size {} is more than file size {}", value, file.size);
                            log_error!(self.ctx, "Failed reading: {}", error,);
                            self.ctx.emit_now(
                                FileReadFailed {
                                    request_id,
                                    file_name: file_name.to_string(),
                                    error,
                                },
                                requester,
                            );

                            return request_id;
                        }
                        value
                    } else {
                        file.size
                    };

                    file.cnt_actions += 1;
                    let disk_request_id = disk.borrow_mut().read(size_to_read, self.ctx.id());
                    self.requests.insert(
                        (disk.borrow().id(), disk_request_id),
                        (request_id, requester, file_name.into()),
                    );
                } else {
                    let error = format!("file [{}] does not exist", file_name);
                    log_error!(self.ctx, "Failed reading: {}", error,);
                    self.ctx.emit_now(
                        FileReadFailed {
                            request_id,
                            file_name: file_name.to_string(),
                            error,
                        },
                        requester,
                    );
                }
            }
            Err(error) => {
                log_error!(self.ctx, "Failed reading: {}", error,);
                self.ctx.emit_now(
                    FileReadFailed {
                        request_id,
                        file_name: file_name.to_string(),
                        error,
                    },
                    requester,
                );
            }
        }
        request_id
    }

    pub fn write(&mut self, file_name: &str, size: u64, requester: Id) -> u64 {
        log_debug!(
            self.ctx,
            "Received write request, size: {}, file: [{}], requester: {}",
            size,
            file_name,
            requester,
        );
        let request_id = self.get_unique_request_id();
        match self.resolve_disk(&file_name) {
            Ok(disk) => {
                if let Some(file) = self.files.get_mut(file_name) {
                    file.cnt_actions += 1;
                    let disk_request_id = disk.borrow_mut().write(size, self.ctx.id());
                    self.requests.insert(
                        (disk.borrow().id(), disk_request_id),
                        (request_id, requester.into(), file_name.into()),
                    );
                } else {
                    let error = format!("file [{}] does not exist", file_name);
                    log_error!(self.ctx, "Failed writing: {}", error,);
                    self.ctx.emit_now(
                        FileWriteFailed {
                            request_id,
                            file_name: file_name.to_string(),
                            error,
                        },
                        requester,
                    );
                }
            }
            Err(error) => {
                log_error!(self.ctx, "Failed writing: {}", error,);
                self.ctx.emit_now(
                    FileWriteFailed {
                        request_id,
                        file_name: file_name.to_string(),
                        error,
                    },
                    requester,
                );
            }
        }
        request_id
    }

    pub fn create_file(&mut self, file_name: &str) -> Result<(), String> {
        log_debug!(self.ctx, "Received create file request, file_name: [{}]", file_name);
        if let Some(_) = self.files.get(file_name) {
            return Err(format!("file [{}] already exists", file_name));
        }
        self.resolve_disk(file_name)?;
        self.files.insert(file_name.to_string(), File::new(0));
        Ok(())
    }

    pub fn get_file_size(&self, file_name: &str) -> Result<u64, String> {
        self.files
            .get(file_name)
            .ok_or(format!("file [{}] does not exist", file_name))
            .map(|f| f.size)
    }

    pub fn get_used_space(&self) -> u64 {
        self.disks.iter().map(|(_, v)| v.borrow().get_used_space()).sum()
    }

    pub fn delete_file(&mut self, file_name: &str) -> Result<(), String> {
        log_debug!(self.ctx, "Received delete file request, file_name: [{}]", file_name);
        let disk = self.resolve_disk(file_name)?;
        let file = self
            .files
            .get(file_name)
            .ok_or(format!("file [{}] does not exist", file_name))?;
        if file.cnt_actions > 0 {
            return Err(format!("file [{}] is busy and cannot be removed", file_name));
        }
        disk.borrow_mut().mark_free(file.size)?;
        self.files.remove(file_name);
        Ok(())
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
                        log_debug!(
                            self.ctx,
                            "Completed reading from file [{}], read size: {}",
                            file_name,
                            size,
                        );
                        file.cnt_actions -= 1;
                        self.ctx.emit_now(
                            FileReadCompleted {
                                request_id: *request_id,
                                file_name: file_name.clone(),
                                read_size: size,
                            },
                            *requester,
                        );
                        self.requests.remove(&key);
                    } else {
                        panic!("File [{}] was lost while reading", file_name);
                    }
                } else {
                    panic!("Request ({},{}) not found", key.0, key.1);
                }
            }
            DataReadFailed {
                request_id: disk_request_id,
                error,
            } => {
                let key = (event.src, disk_request_id);
                if let Some((request_id, requester, file_name)) = self.requests.get(&key) {
                    if let Some(file) = self.files.get_mut(file_name) {
                        log_error!(
                            self.ctx,
                            "Disk failed reading from file [{}], error: {}",
                            file_name,
                            error
                        );
                        file.cnt_actions -= 1;
                        self.ctx.emit_now(
                            FileReadFailed {
                                request_id: *request_id,
                                file_name: file_name.clone(),
                                error,
                            },
                            *requester,
                        );
                        self.requests.remove(&key);
                    } else {
                        panic!("File [{}] was lost while reading", file_name);
                    }
                } else {
                    panic!("Request ({},{}) not found", key.0, key.1);
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
                            "Completed writing to file [{}], written size: {}, new size: {}",
                            file_name,
                            size,
                            file.size,
                        );
                        self.ctx.emit_now(
                            FileWriteCompleted {
                                request_id: *request_id,
                                file_name: file_name.clone(),
                                new_size: file.size,
                            },
                            *requester,
                        );
                        self.requests.remove(&key);
                    } else {
                        panic!("File [{}] was lost while writing", file_name);
                    }
                } else {
                    panic!("Request ({},{}) not found", key.0, key.1);
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
                        log_error!(
                            self.ctx,
                            "Disk failed writing to file [{}], error: {}",
                            file_name,
                            error,
                        );
                        self.ctx.emit_now(
                            FileWriteFailed {
                                request_id: *request_id,
                                file_name: file_name.clone(),
                                error,
                            },
                            *requester,
                        );
                        self.requests.remove(&key);
                    } else {
                        panic!("File [{}] was lost while writing", file_name);
                    }
                } else {
                    panic!("Request ({},{}) not found", key.0, key.1);
                }
            }
        })
    }
}

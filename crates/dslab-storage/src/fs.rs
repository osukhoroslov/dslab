//! File system model.
//!
//! It is built on top of the disk model and supports modeling a storage system on the level of file system operations.
//! The model provides common methods for manipulating the file system such as creation and deletion of files, mounting
//! and unmounting disks, reading and writing files. It also supports modeling a system consisting of multiple disks
//! mounted on distinct mount points.
//!
//! Usage example can be found in `/examples/storage-fs`

use std::{cell::RefCell, collections::HashMap, rc::Rc};

use dslab_core::component::Id;
use dslab_core::{cast, context::SimulationContext, event::Event, handler::EventHandler, log_debug, log_error};

use crate::{disk::Disk, disk::DiskInfo, events::*};

struct File {
    size: u64,
    /// Number of timed actions on this file. File can be removed only if there are no actions on it.
    cnt_actions: u64,
}

impl File {
    fn new(size: u64) -> Self {
        Self { size, cnt_actions: 0 }
    }
}

/// Representation of file system.
pub struct FileSystem {
    files: HashMap<String, File>,
    disks: HashMap<String, Rc<RefCell<Disk>>>,
    /// Mapping (disk id, disk_request_id) -> (request_id, requester, file_path).
    requests: HashMap<(Id, u64), (u64, Id, String)>,
    next_request_id: u64,
    ctx: SimulationContext,
}

impl FileSystem {
    /// Creates new empty file system.
    pub fn new(ctx: SimulationContext) -> Self {
        Self {
            files: HashMap::new(),
            disks: HashMap::new(),
            requests: HashMap::new(),
            next_request_id: 0,
            ctx,
        }
    }

    /// Mounts `disk` to `mount_point` if it is not taken yet.
    pub fn mount_disk(&mut self, mount_point: &str, disk: Rc<RefCell<Disk>>) -> Result<(), String> {
        log_debug!(self.ctx, "Received mount disk request, mount_point: [{}]", mount_point);
        if self.disks.get(mount_point).is_some() {
            return Err(format!("mount point [{}] is already is use", mount_point));
        }
        self.disks.insert(mount_point.to_string(), disk);
        Ok(())
    }

    /// Unmounts a disk which is mounted to `mount_point` if there is any.
    pub fn unmount_disk(&mut self, mount_point: &str) -> Result<(), String> {
        log_debug!(
            self.ctx,
            "Received unmount disk request, mount_point: [{}]",
            mount_point
        );
        if self.disks.remove(mount_point).is_none() {
            return Err(format!("unknown mount point [{}]", mount_point));
        }
        Ok(())
    }

    fn resolve_disk(&self, file_path: &str) -> Result<Rc<RefCell<Disk>>, String> {
        for (mount_point, disk) in &self.disks {
            if file_path.starts_with(mount_point) {
                return Ok(disk.clone());
            }
        }
        Err(format!("cannot resolve on which disk file [{}] is located", file_path))
    }

    fn make_unique_request_id(&mut self) -> u64 {
        let request_id = self.next_request_id;
        self.next_request_id += 1;
        request_id
    }

    /// Submits file read request and returns unique request id.
    ///
    /// The amount of data read from file located at `file_path` is specified in `size`.
    /// The component specified in `requester` will receive `FileReadCompleted` event upon the read completion. If the
    /// read size is larger than the file size, `FileReadFailed` event will be immediately emitted instead.
    /// Note that the returned request id is unique only within the current file system.
    pub fn read(&mut self, file_path: &str, size: u64, requester: Id) -> u64 {
        log_debug!(
            self.ctx,
            "Received read request, size: {}, file: [{}], requester: {}",
            size,
            file_path,
            requester
        );
        self.read_impl(file_path, Some(size), requester)
    }

    /// Submits file read request and returns unique request id.
    ///
    /// The amount of data read from file located at `file_path` is equal to the file size.
    /// The component specified in `requester` will receive `FileReadCompleted` event upon the read completion.
    /// Note that the returned request id is unique only within the current file system.
    pub fn read_all(&mut self, file_path: &str, requester: Id) -> u64 {
        log_debug!(
            self.ctx,
            "Received read request, size: all, file: [{}], requester: {}",
            file_path,
            requester
        );
        self.read_impl(file_path, None, requester)
    }

    fn read_impl(&mut self, file_path: &str, size: Option<u64>, requester: Id) -> u64 {
        let request_id = self.make_unique_request_id();
        match self.resolve_disk(file_path) {
            Ok(disk) => {
                if let Some(file) = self.files.get_mut(file_path) {
                    let size_to_read = if let Some(value) = size {
                        if file.size < value {
                            let error = format!("requested read size {} is more than file size {}", value, file.size);
                            log_error!(self.ctx, "Failed reading: {}", error,);
                            self.ctx.emit_now(
                                FileReadFailed {
                                    request_id,
                                    file_path: file_path.to_string(),
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
                        (request_id, requester, file_path.into()),
                    );
                } else {
                    let error = format!("file [{}] does not exist", file_path);
                    log_error!(self.ctx, "Failed reading: {}", error,);
                    self.ctx.emit_now(
                        FileReadFailed {
                            request_id,
                            file_path: file_path.to_string(),
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
                        file_path: file_path.to_string(),
                        error,
                    },
                    requester,
                );
            }
        }
        request_id
    }

    /// Submits file write request and returns unique request id.
    ///
    /// The amount of data written to file located at `file_path` is specified in `size`.
    /// The component specified in `requester` will receive `FileWriteCompleted` event upon the write completion. If
    /// there is not enough available disk space, `FileWriteFailed` event will be immediately emitted instead.
    /// Note that the returned request id is unique only within the current file system.
    pub fn write(&mut self, file_path: &str, size: u64, requester: Id) -> u64 {
        log_debug!(
            self.ctx,
            "Received write request, size: {}, file: [{}], requester: {}",
            size,
            file_path,
            requester,
        );
        let request_id = self.make_unique_request_id();
        match self.resolve_disk(file_path) {
            Ok(disk) => {
                if let Some(file) = self.files.get_mut(file_path) {
                    file.cnt_actions += 1;
                    let disk_request_id = disk.borrow_mut().write(size, self.ctx.id());
                    self.requests.insert(
                        (disk.borrow().id(), disk_request_id),
                        (request_id, requester, file_path.into()),
                    );
                } else {
                    let error = format!("file [{}] does not exist", file_path);
                    log_error!(self.ctx, "Failed writing: {}", error,);
                    self.ctx.emit_now(
                        FileWriteFailed {
                            request_id,
                            file_path: file_path.to_string(),
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
                        file_path: file_path.to_string(),
                        error,
                    },
                    requester,
                );
            }
        }
        request_id
    }

    /// Creates file at `file_path` if it doesn’t already exist.
    pub fn create_file(&mut self, file_path: &str) -> Result<(), String> {
        log_debug!(self.ctx, "Received create file request, file_path: [{}]", file_path);
        if self.files.get(file_path).is_some() {
            return Err(format!("file [{}] already exists", file_path));
        }
        self.resolve_disk(file_path)?;
        self.files.insert(file_path.to_string(), File::new(0));
        Ok(())
    }

    /// Returns size of the file located at `file_path` if there is any.
    pub fn file_size(&self, file_path: &str) -> Result<u64, String> {
        self.files
            .get(file_path)
            .ok_or(format!("file [{}] does not exist", file_path))
            .map(|f| f.size)
    }

    /// Returns amount of used space on all disks currently mounted to this file system.
    pub fn used_space(&self) -> u64 {
        self.disks.iter().map(|(_, v)| v.borrow().used_space()).sum()
    }

    /// Returns amount of free space on all disks currently mounted to this file system.
    pub fn free_space(&self) -> u64 {
        self.disks.iter().map(|(_, v)| v.borrow().free_space()).sum()
    }

    /// Returns cumulative capacity of all disks currently mounted to this file system.
    pub fn capacity(&self) -> u64 {
        self.disks.iter().map(|(_, v)| v.borrow().capacity()).sum()
    }

    /// Returns vec of disk info associated with mount points.
    pub fn disks_info(&self) -> Vec<(String, DiskInfo)> {
        self.disks
            .iter()
            .map(|(mount_point, disk)| (mount_point.to_owned(), disk.borrow().info()))
            .collect()
    }

    /// Returns disk info for a mount point.
    pub fn disk_info(&self, mount_point: &str) -> Result<DiskInfo, String> {
        self.resolve_disk(mount_point).map(|disk| disk.borrow().info())
    }

    /// Returns mount points present in this file system.
    pub fn mount_points(&self) -> Vec<String> {
        self.disks
            .iter()
            .map(|(mount_point, _)| mount_point.to_owned())
            .collect()
    }

    /// Deletes file located at `file_path` if there is any.    
    pub fn delete_file(&mut self, file_path: &str) -> Result<(), String> {
        log_debug!(self.ctx, "Received delete file request, file_path: [{}]", file_path);
        let disk = self.resolve_disk(file_path)?;
        let file = self
            .files
            .get(file_path)
            .ok_or(format!("file [{}] does not exist", file_path))?;
        if file.cnt_actions > 0 {
            return Err(format!("file [{}] is busy and cannot be removed", file_path));
        }
        disk.borrow_mut().mark_free(file.size)?;
        self.files.remove(file_path);
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
                if let Some((request_id, requester, file_path)) = self.requests.get(&key) {
                    if let Some(file) = self.files.get_mut(file_path) {
                        log_debug!(
                            self.ctx,
                            "Completed reading from file [{}], read size: {}",
                            file_path,
                            size,
                        );
                        file.cnt_actions -= 1;
                        self.ctx.emit_now(
                            FileReadCompleted {
                                request_id: *request_id,
                                file_path: file_path.clone(),
                                read_size: size,
                            },
                            *requester,
                        );
                        self.requests.remove(&key);
                    } else {
                        panic!("File [{}] was lost while reading", file_path);
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
                if let Some((request_id, requester, file_path)) = self.requests.get(&key) {
                    if let Some(file) = self.files.get_mut(file_path) {
                        log_error!(
                            self.ctx,
                            "Disk failed reading from file [{}], error: {}",
                            file_path,
                            error
                        );
                        file.cnt_actions -= 1;
                        self.ctx.emit_now(
                            FileReadFailed {
                                request_id: *request_id,
                                file_path: file_path.clone(),
                                error,
                            },
                            *requester,
                        );
                        self.requests.remove(&key);
                    } else {
                        panic!("File [{}] was lost while reading", file_path);
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
                if let Some((request_id, requester, file_path)) = self.requests.get(&key) {
                    if let Some(file) = self.files.get_mut(file_path) {
                        file.size += size;
                        file.cnt_actions -= 1;
                        log_debug!(
                            self.ctx,
                            "Completed writing to file [{}], written size: {}, new size: {}",
                            file_path,
                            size,
                            file.size,
                        );
                        self.ctx.emit_now(
                            FileWriteCompleted {
                                request_id: *request_id,
                                file_path: file_path.clone(),
                                new_size: file.size,
                            },
                            *requester,
                        );
                        self.requests.remove(&key);
                    } else {
                        panic!("File [{}] was lost while writing", file_path);
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
                if let Some((request_id, requester, file_path)) = self.requests.get(&key) {
                    if let Some(file) = self.files.get_mut(file_path) {
                        file.cnt_actions -= 1;
                        log_error!(
                            self.ctx,
                            "Disk failed writing to file [{}], error: {}",
                            file_path,
                            error,
                        );
                        self.ctx.emit_now(
                            FileWriteFailed {
                                request_id: *request_id,
                                file_path: file_path.clone(),
                                error,
                            },
                            *requester,
                        );
                        self.requests.remove(&key);
                    } else {
                        panic!("File [{}] was lost while writing", file_path);
                    }
                } else {
                    panic!("Request ({},{}) not found", key.0, key.1);
                }
            }
        })
    }
}

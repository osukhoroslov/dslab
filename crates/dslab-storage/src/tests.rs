use std::cell::RefCell;
use std::rc::Rc;

use sugars::{rc, refcell};

use dslab_core::simulation::Simulation;
use dslab_core::{cast, Event, EventHandler};

use crate::disk::{Disk, DiskBuilder};
use crate::events::*;
use crate::fs::FileSystem;
use crate::storage::{Storage, StorageInfo};

///////////////////////////////////////////////////////////////////////////////

const SEED: u64 = 16;
const DISK_CAPACITY: u64 = 100;
const DISK_READ_BW: f64 = 100.;
const DISK_WRITE_BW: f64 = 100.;

///////////////////////////////////////////////////////////////////////////////

fn make_filesystem(sim: &mut Simulation, name: &str) -> Rc<RefCell<FileSystem>> {
    let fs = rc!(refcell!(FileSystem::new(sim.create_context(name))));
    sim.add_handler(name, fs.clone());
    fs
}

fn make_simple_disk(sim: &mut Simulation, name: &str) -> Rc<RefCell<Disk>> {
    let disk = rc!(refcell!(DiskBuilder::simple(
        DISK_CAPACITY,
        DISK_READ_BW,
        DISK_WRITE_BW,
    )
    .build(sim.create_context(name))));
    sim.add_handler(name, disk.clone());
    disk
}

///////////////////////////////////////////////////////////////////////////////

#[derive(PartialEq)]
enum ExpectedEventType {
    DataReadCompleted,
    DataReadFailed,
    DataWriteCompleted,
    DataWriteFailed,
    FileReadCompleted,
    FileReadFailed,
    FileWriteCompleted,
    FileWriteFailed,
}

struct Checker {
    expected_event_type: ExpectedEventType,
    received_events_count: u64,
}

impl Checker {
    fn new(expected_event_type: ExpectedEventType) -> Checker {
        Checker {
            expected_event_type,
            received_events_count: 0,
        }
    }

    fn received_events_count(&self) -> u64 {
        self.received_events_count
    }
}

impl EventHandler for Checker {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            FileReadCompleted { .. } => {
                if self.expected_event_type != ExpectedEventType::FileReadCompleted {
                    panic!();
                }
            }
            FileReadFailed { .. } => {
                if self.expected_event_type != ExpectedEventType::FileReadFailed {
                    panic!();
                }
            }
            FileWriteCompleted { .. } => {
                if self.expected_event_type != ExpectedEventType::FileWriteCompleted {
                    panic!();
                }
            }
            FileWriteFailed { .. } => {
                if self.expected_event_type != ExpectedEventType::FileWriteFailed {
                    panic!();
                }
            }
            DataReadCompleted { .. } => {
                if self.expected_event_type != ExpectedEventType::DataReadCompleted {
                    panic!();
                }
            }
            DataReadFailed { .. } => {
                if self.expected_event_type != ExpectedEventType::DataReadFailed {
                    panic!();
                }
            }
            DataWriteCompleted { .. } => {
                if self.expected_event_type != ExpectedEventType::DataWriteCompleted {
                    panic!();
                }
            }
            DataWriteFailed { .. } => {
                if self.expected_event_type != ExpectedEventType::DataWriteFailed {
                    panic!();
                }
            }
        });
        self.received_events_count += 1;
    }
}

///////////////////////////////////////////////////////////////////////////////

#[test]
fn fs_files_metadata_consistence() {
    let mut sim = Simulation::new(SEED);

    let checker = rc!(refcell!(Checker::new(ExpectedEventType::FileWriteCompleted)));
    let checker_id = sim.add_handler("User", checker);

    let fs = make_filesystem(&mut sim, "FS-1");
    let disk = make_simple_disk(&mut sim, "Disk-1");

    assert!(fs.borrow_mut().create_file("/mnt/file1").is_err());
    assert!(fs.borrow().file_size("/mnt/file1").is_err());

    assert_eq!(fs.borrow().capacity(), 0);
    assert_eq!(fs.borrow().used_space(), 0);
    assert_eq!(fs.borrow().free_space(), 0);
    assert_eq!(fs.borrow().disks_info(), vec![]);

    assert!(fs.borrow_mut().mount_disk("/mnt", disk.clone()).is_ok());
    assert!(fs.borrow_mut().create_file("/mnt/file1").is_ok());

    assert_eq!(fs.borrow().file_size("/mnt/file1"), Ok(0));

    assert_eq!(fs.borrow().capacity(), DISK_CAPACITY);
    assert_eq!(fs.borrow().used_space(), 0);
    assert_eq!(fs.borrow().free_space(), DISK_CAPACITY);
    assert_eq!(
        fs.borrow().disks_info(),
        vec![(
            "/mnt".to_owned(),
            StorageInfo {
                capacity: DISK_CAPACITY,
                used_space: 0,
                free_space: DISK_CAPACITY
            }
        )]
    );

    fs.borrow_mut().write("/mnt/file1", 1, checker_id);

    sim.step_until_no_events();

    assert_eq!(fs.borrow().file_size("/mnt/file1"), Ok(1));

    assert_eq!(fs.borrow().capacity(), DISK_CAPACITY);
    assert_eq!(fs.borrow().used_space(), 1);
    assert_eq!(fs.borrow().free_space(), DISK_CAPACITY - 1);
    assert_eq!(
        fs.borrow().disks_info(),
        vec![(
            "/mnt".to_owned(),
            StorageInfo {
                capacity: DISK_CAPACITY,
                used_space: 1,
                free_space: DISK_CAPACITY - 1
            }
        )]
    );

    assert!(fs.borrow_mut().create_file("/mnt/file2").is_ok());
    fs.borrow_mut().write("/mnt/file2", 2, checker_id);
    assert!(fs.borrow_mut().delete_file("/mnt/file2").is_err());

    sim.step_until_no_events();

    assert_eq!(fs.borrow().file_size("/mnt/file2"), Ok(2));
    assert_eq!(fs.borrow().capacity(), DISK_CAPACITY);
    assert_eq!(fs.borrow().used_space(), 3);
    assert_eq!(fs.borrow().free_space(), DISK_CAPACITY - 3);
    assert_eq!(
        fs.borrow().disks_info(),
        vec![(
            "/mnt".to_owned(),
            StorageInfo {
                capacity: DISK_CAPACITY,
                used_space: 3,
                free_space: DISK_CAPACITY - 3,
            }
        )]
    );

    assert!(fs.borrow_mut().delete_file("/mnt/file2").is_ok());
    assert!(fs.borrow_mut().delete_file("/mnt/file2").is_err());
    assert!(fs.borrow().file_size("/mnt/file2").is_err());

    assert_eq!(fs.borrow().capacity(), DISK_CAPACITY);
    assert_eq!(fs.borrow().used_space(), 1);
    assert_eq!(fs.borrow().free_space(), DISK_CAPACITY - 1);
    assert_eq!(
        fs.borrow().disks_info(),
        vec![(
            "/mnt".to_owned(),
            StorageInfo {
                capacity: DISK_CAPACITY,
                used_space: 1,
                free_space: DISK_CAPACITY - 1,
            }
        )]
    );
}

#[test]
fn fs_multiple_disks_on_single_filesystem() {
    let mut sim = Simulation::new(SEED);

    let checker_ok = rc!(refcell!(Checker::new(ExpectedEventType::FileWriteCompleted)));
    let checker_ok_id = sim.add_handler("User1", checker_ok);

    let fs = make_filesystem(&mut sim, "FS-1");
    let disk1 = make_simple_disk(&mut sim, "Disk-1");
    let disk2 = make_simple_disk(&mut sim, "Disk-2");

    // Disk is not mounted yet
    assert!(fs.borrow_mut().unmount_disk("/mnt/vda").is_err());

    assert!(fs.borrow_mut().mount_disk("/mnt/vda", disk1.clone()).is_ok());
    assert!(fs.borrow_mut().unmount_disk("/mnt/vda").is_ok());
    assert!(fs.borrow_mut().mount_disk("/mnt/vda", disk1.clone()).is_ok());

    assert!(fs.borrow_mut().mount_disk("/mnt/vdb", disk2.clone()).is_ok());

    assert_eq!(disk1.borrow().used_space(), 0);
    assert_eq!(disk2.borrow().used_space(), 0);
    assert_eq!(fs.borrow().used_space(), 0);

    assert!(fs.borrow_mut().create_file("/mnt/vda/file1").is_ok());
    assert!(fs.borrow_mut().create_file("/mnt/vdb/file2").is_ok());

    fs.borrow_mut().write("/mnt/vda/file1", 2, checker_ok_id);
    fs.borrow_mut().write("/mnt/vdb/file2", 3, checker_ok_id);

    sim.step_until_no_events();

    assert_eq!(disk1.borrow().used_space(), 2);
    assert_eq!(disk2.borrow().used_space(), 3);
    assert_eq!(fs.borrow().used_space(), 5);

    assert!(fs.borrow_mut().delete_file("/mnt/vdb/file2").is_ok());

    assert_eq!(disk1.borrow().used_space(), 2);
    assert_eq!(disk2.borrow().used_space(), 0);
    assert_eq!(fs.borrow().used_space(), 2);
}

#[test]
fn fs_single_disk_on_multiple_filesystems() {
    let mut sim = Simulation::new(SEED);

    let fs1 = make_filesystem(&mut sim, "FS-1");
    let fs2 = make_filesystem(&mut sim, "FS-2");
    let disk = make_simple_disk(&mut sim, "Disk-1");

    assert!(fs1.borrow_mut().unmount_disk("/mnt").is_err());
    assert!(fs2.borrow_mut().unmount_disk("/mnt").is_err());

    assert!(fs1.borrow_mut().mount_disk("/mnt/vdc", disk.clone()).is_ok());
    assert!(fs2.borrow_mut().mount_disk("/mnt/vda", disk.clone()).is_ok());

    assert!(fs1.borrow_mut().unmount_disk("/mnt/vdc").is_ok());
    assert!(fs1.borrow_mut().mount_disk("/mnt/vda", disk.clone()).is_ok());

    assert_eq!(disk.borrow().used_space(), 0);
    assert_eq!(fs1.borrow().used_space(), 0);
    assert_eq!(fs2.borrow().used_space(), 0);

    assert!(fs1.borrow_mut().create_file("/mnt/vda/file").is_ok());
    fs1.borrow_mut().write("/mnt/vda/file", 4, 0);

    sim.step_until_no_events();

    // Used space on shared disk is visible for both file systems
    assert_eq!(disk.borrow().used_space(), 4);
    assert_eq!(fs1.borrow().used_space(), 4);
    assert_eq!(fs2.borrow().used_space(), 4);

    assert!(fs1.borrow_mut().unmount_disk("/mnt/vda").is_ok());
    assert!(fs2.borrow_mut().unmount_disk("/mnt/vda").is_ok());

    // Used space on disk does not change after unmount
    assert_eq!(disk.borrow().used_space(), 4);
}

#[test]
fn fs_good_read_write() {
    let mut sim = Simulation::new(SEED);

    let checker = rc!(refcell!(Checker::new(ExpectedEventType::FileWriteCompleted)));
    let checker_id = sim.add_handler("User", checker);

    let fs = make_filesystem(&mut sim, "FS-1");
    let disk = make_simple_disk(&mut sim, "Disk-1");

    assert!(fs.borrow_mut().mount_disk("/mnt", disk).is_ok());
    assert!(fs.borrow_mut().create_file("/mnt/file").is_ok());

    fs.borrow_mut().write("/mnt/file", 99, checker_id);
    sim.step_until_no_events();

    let read_checker = rc!(refcell!(Checker::new(ExpectedEventType::FileReadCompleted)));
    let read_checker_id = sim.add_handler("User", read_checker);

    fs.borrow_mut().read("/mnt/file", 99, read_checker_id);
    fs.borrow_mut().read_all("/mnt/file", read_checker_id);

    sim.step_until_no_events();
}

#[test]
fn fs_failed_read_non_existent_file() {
    let mut sim = Simulation::new(SEED);

    let checker = rc!(refcell!(Checker::new(ExpectedEventType::FileReadFailed)));
    let checker_id = sim.add_handler("User", checker);

    let fs = make_filesystem(&mut sim, "FS-1");
    let disk = make_simple_disk(&mut sim, "Disk-1");

    assert!(fs.borrow_mut().mount_disk("/mnt", disk).is_ok());

    fs.borrow_mut().read_all("/mnt/file", checker_id);
    sim.step_until_no_events();
}

#[test]
fn fs_failed_read_unmounted_disk() {
    let mut sim = Simulation::new(SEED);

    let checker_ok = rc!(refcell!(Checker::new(ExpectedEventType::FileWriteCompleted)));
    let checker_ok_id = sim.add_handler("User1", checker_ok);

    let checker_fail = rc!(refcell!(Checker::new(ExpectedEventType::FileReadFailed)));
    let checker_fail_id = sim.add_handler("User2", checker_fail);

    let fs = make_filesystem(&mut sim, "FS-1");
    let disk = make_simple_disk(&mut sim, "Disk-1");

    assert!(fs.borrow_mut().mount_disk("/mnt", disk).is_ok());
    assert!(fs.borrow_mut().create_file("/mnt/file").is_ok());

    fs.borrow_mut().write("/mnt/file", 10, checker_ok_id);
    sim.step_until_no_events();

    assert!(fs.borrow_mut().unmount_disk("/mnt").is_ok());

    fs.borrow_mut().read_all("/mnt/file", checker_fail_id);
    sim.step_until_no_events();
}

#[test]
fn fs_failed_read_file_bad_size() {
    let mut sim = Simulation::new(SEED);

    let checker = rc!(refcell!(Checker::new(ExpectedEventType::FileWriteCompleted)));
    let checker_id = sim.add_handler("User", checker);

    let fs = make_filesystem(&mut sim, "FS-1");
    let disk = make_simple_disk(&mut sim, "Disk-1");

    assert!(fs.borrow_mut().mount_disk("/mnt", disk).is_ok());
    assert!(fs.borrow_mut().create_file("/mnt/file").is_ok());

    fs.borrow_mut().write("/mnt/file", 98, checker_id);
    sim.step_until_no_events();

    let read_checker = rc!(refcell!(Checker::new(ExpectedEventType::FileReadFailed)));
    let read_checker_id = sim.add_handler("User", read_checker);

    fs.borrow_mut().read("/mnt/file", 99, read_checker_id);
    sim.step_until_no_events();
}

#[test]
fn fs_failed_write_unresolved_disk() {
    let mut sim = Simulation::new(SEED);

    let checker = rc!(refcell!(Checker::new(ExpectedEventType::FileWriteFailed)));
    let checker_id = sim.add_handler("User", checker);

    let fs = make_filesystem(&mut sim, "FS-1");

    fs.borrow_mut().write("/mnt/file", 99, checker_id);
    sim.step_until_no_events();
}

// Write fails because of non-existent file
#[test]
fn fs_failed_write_non_existent_file() {
    let mut sim = Simulation::new(SEED);

    let checker = rc!(refcell!(Checker::new(ExpectedEventType::FileWriteFailed)));
    let checker_id = sim.add_handler("User", checker);

    let fs = make_filesystem(&mut sim, "FS-1");
    let disk = make_simple_disk(&mut sim, "Disk-1");

    assert!(fs.borrow_mut().mount_disk("/mnt", disk).is_ok());

    fs.borrow_mut().write("/mnt/file", 99, checker_id);
    sim.step_until_no_events();
}

// Write fails because of low disk capacity
#[test]
fn fs_failed_write_low_disk_capacity() {
    let mut sim = Simulation::new(SEED);

    let checker = rc!(refcell!(Checker::new(ExpectedEventType::FileWriteFailed)));
    let checker_id = sim.add_handler("User", checker);

    let fs = make_filesystem(&mut sim, "FS-1");
    let disk = make_simple_disk(&mut sim, "Disk-1");

    assert!(fs.borrow_mut().mount_disk("/mnt", disk).is_ok());
    assert!(fs.borrow_mut().create_file("/mnt/file").is_ok());

    fs.borrow_mut().write("/mnt/file", 101, checker_id);
    sim.step_until_no_events();
}

// Disk tests

#[test]
fn disk_good_read_write_with_time_check() {
    let mut sim = Simulation::new(SEED);

    let checker_read = rc!(refcell!(Checker::new(ExpectedEventType::DataReadCompleted)));
    let checker_read_id = sim.add_handler("User-1", checker_read);

    let checker_write = rc!(refcell!(Checker::new(ExpectedEventType::DataWriteCompleted)));
    let checker_write_id = sim.add_handler("User-2", checker_write);

    let disk = make_simple_disk(&mut sim, "Disk-1");

    disk.borrow_mut().write(2, checker_write_id);
    sim.step_until_no_events();

    assert_eq!(sim.time(), 2. / DISK_WRITE_BW);

    disk.borrow_mut().read(2, checker_read_id);
    sim.step_until_no_events();

    assert_eq!(sim.time(), 2. / DISK_READ_BW + 2. / DISK_WRITE_BW);

    disk.borrow_mut().read(1, checker_read_id);
    sim.step_until_no_events();

    assert_eq!(sim.time(), 2. / DISK_READ_BW + 2. / DISK_WRITE_BW + 1. / DISK_WRITE_BW);
}

// Read fails because of too big requested size
#[test]
fn disk_failed_read_bad_size() {
    let mut sim = Simulation::new(SEED);

    let checker = rc!(refcell!(Checker::new(ExpectedEventType::DataReadFailed)));
    let checker_id = sim.add_handler("User", checker);

    let disk = make_simple_disk(&mut sim, "Disk-1");

    disk.borrow_mut().read(101, checker_id);
    sim.step_until_no_events();
}

// Write fails because of low disk capacity
#[test]
fn disk_failed_write_low_disk_capacity() {
    let mut sim = Simulation::new(SEED);

    let checker = rc!(refcell!(Checker::new(ExpectedEventType::DataWriteFailed)));
    let checker_id = sim.add_handler("User", checker);

    let disk = make_simple_disk(&mut sim, "Disk-1");

    disk.borrow_mut().write(101, checker_id);
    sim.step_until_no_events();
}

// Disk space is marked free correctly
#[test]
fn disk_write_after_spaced_marked_free() {
    let mut sim = Simulation::new(SEED);

    let checker = rc!(refcell!(Checker::new(ExpectedEventType::DataWriteCompleted)));
    let checker_id = sim.add_handler("User", checker);

    let disk = make_simple_disk(&mut sim, "Disk-1");

    disk.borrow_mut().write(99, checker_id);
    sim.step_until_no_events();

    assert!(disk.borrow().capacity() == DISK_CAPACITY);
    assert!(disk.borrow().used_space() == 99);
    assert!(disk.borrow().free_space() == DISK_CAPACITY - 99);

    assert!(disk.borrow_mut().mark_free(100).is_err());
    assert!(disk.borrow_mut().mark_free(99).is_ok());

    assert!(disk.borrow().capacity() == DISK_CAPACITY);
    assert!(disk.borrow().used_space() == 0);
    assert!(disk.borrow().free_space() == DISK_CAPACITY);

    disk.borrow_mut().write(99, checker_id);
    sim.step_until_no_events();

    assert!(disk.borrow().capacity() == DISK_CAPACITY);
    assert!(disk.borrow().used_space() == 99);
    assert!(disk.borrow().free_space() == DISK_CAPACITY - 99);
}

#[test]
fn disk_with_total_ops_limit() {
    let mut sim = Simulation::new(SEED);

    let write_checker = rc!(refcell!(Checker::new(ExpectedEventType::DataWriteCompleted)));
    let write_checker_id = sim.add_handler("Writer", write_checker.clone());

    let read_checker = rc!(refcell!(Checker::new(ExpectedEventType::DataReadCompleted)));
    let read_checker_id = sim.add_handler("Reader", read_checker.clone());

    let disk = rc!(refcell!(DiskBuilder::simple(
        DISK_CAPACITY,
        DISK_READ_BW,
        DISK_WRITE_BW,
    )
    .concurrent_ops_limit(1)
    .build(sim.create_context("Disk-1"))));

    sim.add_handler("Disk-1", disk.clone());

    disk.borrow_mut().write(50, write_checker_id);
    disk.borrow_mut().read(100, read_checker_id);
    disk.borrow_mut().write(50, write_checker_id);
    disk.borrow_mut().read(100, read_checker_id);

    assert_eq!(write_checker.borrow().received_events_count(), 0);
    assert_eq!(read_checker.borrow().received_events_count(), 0);

    sim.step_for_duration(0.5);
    assert_eq!(write_checker.borrow().received_events_count(), 1);
    assert_eq!(read_checker.borrow().received_events_count(), 0);

    sim.step_for_duration(0.5);
    assert_eq!(write_checker.borrow().received_events_count(), 1);
    assert_eq!(read_checker.borrow().received_events_count(), 0);

    sim.step_for_duration(0.5);
    assert_eq!(write_checker.borrow().received_events_count(), 1);
    assert_eq!(read_checker.borrow().received_events_count(), 1);

    sim.step_for_duration(0.5);
    assert_eq!(write_checker.borrow().received_events_count(), 2);
    assert_eq!(read_checker.borrow().received_events_count(), 1);

    sim.step_for_duration(0.5);
    assert_eq!(write_checker.borrow().received_events_count(), 2);
    assert_eq!(read_checker.borrow().received_events_count(), 1);

    sim.step_for_duration(0.5);
    assert_eq!(write_checker.borrow().received_events_count(), 2);
    assert_eq!(read_checker.borrow().received_events_count(), 2);

    sim.step_until_no_events();
    assert_eq!(write_checker.borrow().received_events_count(), 2);
    assert_eq!(read_checker.borrow().received_events_count(), 2);
}

#[test]
fn disk_with_read_write_ops_limit() {
    let mut sim = Simulation::new(SEED);

    let write_checker = rc!(refcell!(Checker::new(ExpectedEventType::DataWriteCompleted)));
    let write_checker_id = sim.add_handler("Writer", write_checker.clone());

    let read_checker = rc!(refcell!(Checker::new(ExpectedEventType::DataReadCompleted)));
    let read_checker_id = sim.add_handler("Reader", read_checker.clone());

    let disk = rc!(refcell!(DiskBuilder::simple(
        DISK_CAPACITY,
        DISK_READ_BW,
        DISK_WRITE_BW,
    )
    .concurrent_read_ops_limit(1)
    .concurrent_write_ops_limit(1)
    .build(sim.create_context("Disk-1"))));

    sim.add_handler("Disk-1", disk.clone());

    disk.borrow_mut().write(50, write_checker_id);
    disk.borrow_mut().read(100, read_checker_id);
    disk.borrow_mut().write(50, write_checker_id);
    disk.borrow_mut().read(100, read_checker_id);

    assert_eq!(write_checker.borrow().received_events_count(), 0);
    assert_eq!(read_checker.borrow().received_events_count(), 0);

    sim.step_for_duration(0.5);
    assert_eq!(write_checker.borrow().received_events_count(), 1);
    assert_eq!(read_checker.borrow().received_events_count(), 0);

    sim.step_for_duration(0.5);
    assert_eq!(write_checker.borrow().received_events_count(), 2);
    assert_eq!(read_checker.borrow().received_events_count(), 1);

    sim.step_for_duration(0.5);
    assert_eq!(write_checker.borrow().received_events_count(), 2);
    assert_eq!(read_checker.borrow().received_events_count(), 1);

    sim.step_for_duration(0.5);
    assert_eq!(write_checker.borrow().received_events_count(), 2);
    assert_eq!(read_checker.borrow().received_events_count(), 2);

    sim.step_until_no_events();
    assert_eq!(write_checker.borrow().received_events_count(), 2);
    assert_eq!(read_checker.borrow().received_events_count(), 2);
}

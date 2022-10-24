use std::cell::RefCell;
use std::rc::Rc;

use sugars::{rc, refcell};

use dslab_core::simulation::Simulation;
use dslab_core::{cast, Event, EventHandler};

use crate::disk::Disk;
use crate::events::{FileReadCompleted, FileReadFailed, FileWriteCompleted, FileWriteFailed};
use crate::fs::FileSystem;

///////////////////////////////////////////////////////////////////////////////

const SEED: u64 = 16;
const DISK_CAPACITY: u64 = 100;
const DISK_READ_BW: u64 = 100;
const DISK_WRITE_BW: u64 = 100;

///////////////////////////////////////////////////////////////////////////////

fn make_filesystem(sim: &mut Simulation, name: &str) -> Rc<RefCell<FileSystem>> {
    let fs = rc!(refcell!(FileSystem::new(sim.create_context(name))));
    sim.add_handler(name, fs.clone());

    fs
}

fn make_disk(sim: &mut Simulation, name: &str) -> Rc<RefCell<Disk>> {
    rc!(refcell!(Disk::new_simple(
        DISK_CAPACITY,
        DISK_READ_BW,
        DISK_WRITE_BW,
        sim.create_context(name),
    )))
}

///////////////////////////////////////////////////////////////////////////////

#[derive(PartialEq)]
enum ExpectedEventType {
    ReadCompleted,
    ReadFailed,
    WriteCompleted,
    WriteFailed,
}

struct Checker {
    expected_event_type: ExpectedEventType,
}

impl Checker {
    fn new(expected_event_type: ExpectedEventType) -> Checker {
        Checker { expected_event_type }
    }
}

impl EventHandler for Checker {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            FileReadCompleted { .. } => {
                if self.expected_event_type != ExpectedEventType::ReadCompleted {
                    panic!();
                }
            }
            FileReadFailed { .. } => {
                if self.expected_event_type != ExpectedEventType::ReadFailed {
                    panic!();
                }
            }
            FileWriteCompleted { .. } => {
                if self.expected_event_type != ExpectedEventType::WriteCompleted {
                    panic!();
                }
            }
            FileWriteFailed { .. } => {
                if self.expected_event_type != ExpectedEventType::WriteFailed {
                    panic!();
                }
            }
        })
    }
}

///////////////////////////////////////////////////////////////////////////////

#[test]
fn files_metadata_consistence() {
    let mut sim = Simulation::new(SEED);

    let checker = rc!(refcell!(Checker::new(ExpectedEventType::WriteCompleted)));
    let checker_id = sim.add_handler("User", checker);

    let fs = make_filesystem(&mut sim, "FS-1");
    let disk = make_disk(&mut sim, "Disk-1");

    assert!(fs.borrow_mut().create_file("/mnt/file1").is_err());
    assert!(fs.borrow().get_file_size("/mnt/file1").is_err());
    assert_eq!(fs.borrow().get_used_space(), 0);

    assert!(fs.borrow_mut().mount_disk("/mnt", disk.clone()).is_ok());
    assert!(fs.borrow_mut().create_file("/mnt/file1").is_ok());

    assert_eq!(fs.borrow().get_file_size("/mnt/file1"), Ok(0));
    assert_eq!(fs.borrow().get_used_space(), 0);

    fs.borrow_mut().write("/mnt/file1", 1, checker_id);

    sim.step_until_no_events();

    assert_eq!(fs.borrow().get_file_size("/mnt/file1"), Ok(1));
    assert_eq!(fs.borrow().get_used_space(), 1);

    assert!(fs.borrow_mut().create_file("/mnt/file2").is_ok());
    fs.borrow_mut().write("/mnt/file2", 2, checker_id);
    assert!(fs.borrow_mut().delete_file("/mnt/file2").is_err());

    sim.step_until_no_events();

    assert_eq!(fs.borrow().get_file_size("/mnt/file2"), Ok(2));
    assert_eq!(fs.borrow().get_used_space(), 3);

    assert!(fs.borrow_mut().delete_file("/mnt/file2").is_ok());
    assert!(fs.borrow_mut().delete_file("/mnt/file2").is_err());
    assert!(fs.borrow().get_file_size("/mnt/file2").is_err());
    assert_eq!(fs.borrow().get_used_space(), 1);
}

#[test]
fn multiple_disks_on_single_filesystem() {
    let mut sim = Simulation::new(SEED);

    let fs = make_filesystem(&mut sim, "FS-1");
    let disk1 = make_disk(&mut sim, "Disk-1");
    let disk2 = make_disk(&mut sim, "Disk-2");

    // Disk is not mounted yet
    assert!(fs.borrow_mut().unmount_disk("/mnt/vda").is_err());

    assert!(fs.borrow_mut().mount_disk("/mnt/vda", disk1.clone()).is_ok());
    assert!(fs.borrow_mut().unmount_disk("/mnt/vda").is_ok());
    assert!(fs.borrow_mut().mount_disk("/mnt/vda", disk1.clone()).is_ok());

    // `/mnt` is prefix of `/mnt/vda`, so it can't be used as a mount point
    assert!(fs.borrow_mut().mount_disk("/mnt", disk2.clone()).is_err());

    assert!(fs.borrow_mut().mount_disk("/mnt/vdb", disk2.clone()).is_ok());

    assert_eq!(disk1.borrow().get_used_space(), 0);
    assert_eq!(disk2.borrow().get_used_space(), 0);
    assert_eq!(fs.borrow().get_used_space(), 0);

    fs.borrow_mut().write("/mnt/vda/file1", 2, 0);
    fs.borrow_mut().write("/mnt/vdb/file2", 3, 0);

    sim.step_until_no_events();

    assert_eq!(disk1.borrow().get_used_space(), 2);
    assert_eq!(disk2.borrow().get_used_space(), 3);
    assert_eq!(fs.borrow().get_used_space(), 5);

    assert!(fs.borrow_mut().delete_file("/mnt/vdb/file2").is_ok());

    assert_eq!(disk1.borrow().get_used_space(), 2);
    assert_eq!(disk2.borrow().get_used_space(), 0);
    assert_eq!(fs.borrow().get_used_space(), 2);
}

#[test]
fn single_disk_on_multiple_filesystems() {
    let mut sim = Simulation::new(SEED);

    let fs1 = make_filesystem(&mut sim, "FS-1");
    let fs2 = make_filesystem(&mut sim, "FS-2");
    let disk = make_disk(&mut sim, "Disk-1");

    assert!(fs1.borrow_mut().unmount_disk("/mnt").is_err());
    assert!(fs2.borrow_mut().unmount_disk("/mnt").is_err());

    assert!(fs1.borrow_mut().mount_disk("/mnt/vdc", disk.clone()).is_ok());
    assert!(fs2.borrow_mut().mount_disk("/mnt/vda", disk.clone()).is_ok());

    assert!(fs1.borrow_mut().unmount_disk("/mnt/vdc").is_ok());
    assert!(fs1.borrow_mut().mount_disk("/mnt/vda", disk.clone()).is_ok());

    assert_eq!(disk.borrow().get_used_space(), 0);
    assert_eq!(fs1.borrow().get_used_space(), 0);
    assert_eq!(fs2.borrow().get_used_space(), 0);

    assert!(fs1.borrow_mut().create_file("/mnt/vda/file").is_ok());
    fs1.borrow_mut().write("/mnt/vda/file", 4, 0);

    sim.step_until_no_events();

    // Used space on shared disk is visible for both file systems
    assert_eq!(disk.borrow().get_used_space(), 4);
    assert_eq!(fs1.borrow().get_used_space(), 4);
    assert_eq!(fs2.borrow().get_used_space(), 4);

    assert!(fs1.borrow_mut().unmount_disk("/mnt/vda").is_ok());
    assert!(fs2.borrow_mut().unmount_disk("/mnt/vda").is_ok());

    // Used space on disk does not change after unmount
    assert_eq!(disk.borrow().get_used_space(), 4);
}

#[test]
fn read_write() {
    // Good read and write
    {
        let mut sim = Simulation::new(SEED);

        let checker = rc!(refcell!(Checker::new(ExpectedEventType::WriteCompleted)));
        let checker_id = sim.add_handler("User", checker);

        let fs = make_filesystem(&mut sim, "FS-1");
        let disk = make_disk(&mut sim, "Disk-1");

        assert!(fs.borrow_mut().mount_disk("/mnt", disk).is_ok());
        assert!(fs.borrow_mut().create_file("/mnt/file").is_ok());

        fs.borrow_mut().write("/mnt/file", 99, checker_id);
        sim.step_until_no_events();

        let read_checker = rc!(refcell!(Checker::new(ExpectedEventType::ReadCompleted)));
        let read_checker_id = sim.add_handler("User", read_checker);

        fs.borrow_mut().read("/mnt/file", 99, read_checker_id);
        fs.borrow_mut().read_all("/mnt/file", read_checker_id);

        sim.step_until_no_events();
    }

    // Read failes because file does not exist
    {
        let mut sim = Simulation::new(SEED);

        let checker = rc!(refcell!(Checker::new(ExpectedEventType::ReadFailed)));
        let checker_id = sim.add_handler("User", checker);

        let fs = make_filesystem(&mut sim, "FS-1");
        let disk = make_disk(&mut sim, "Disk-1");

        assert!(fs.borrow_mut().mount_disk("/mnt", disk).is_ok());

        fs.borrow_mut().read_all("/mnt/file", checker_id);
        sim.step_until_no_events();
    }

    // Read failes because of bad size
    {
        let mut sim = Simulation::new(SEED);

        let checker = rc!(refcell!(Checker::new(ExpectedEventType::WriteCompleted)));
        let checker_id = sim.add_handler("User", checker);

        let fs = make_filesystem(&mut sim, "FS-1");
        let disk = make_disk(&mut sim, "Disk-1");

        assert!(fs.borrow_mut().mount_disk("/mnt", disk).is_ok());
        assert!(fs.borrow_mut().create_file("/mnt/file").is_ok());

        fs.borrow_mut().write("/mnt/file", 98, checker_id);
        sim.step_until_no_events();

        let read_checker = rc!(refcell!(Checker::new(ExpectedEventType::ReadFailed)));
        let read_checker_id = sim.add_handler("User", read_checker);

        fs.borrow_mut().read("/mnt/file", 99, read_checker_id);
        sim.step_until_no_events();
    }

    // Write fails because of unresolved disk
    {
        let mut sim = Simulation::new(SEED);

        let checker = rc!(refcell!(Checker::new(ExpectedEventType::WriteFailed)));
        let checker_id = sim.add_handler("User", checker);

        let fs = make_filesystem(&mut sim, "FS-1");

        fs.borrow_mut().write("/mnt/file", 99, checker_id);
        sim.step_until_no_events();
    }

    // Write fails because of non-existent file
    {
        let mut sim = Simulation::new(SEED);

        let checker = rc!(refcell!(Checker::new(ExpectedEventType::WriteFailed)));
        let checker_id = sim.add_handler("User", checker);

        let fs = make_filesystem(&mut sim, "FS-1");
        let disk = make_disk(&mut sim, "Disk-1");

        assert!(fs.borrow_mut().mount_disk("/mnt", disk).is_ok());

        fs.borrow_mut().write("/mnt/file", 99, checker_id);
        sim.step_until_no_events();
    }

    // Write fails because of low disk capacity
    {
        let mut sim = Simulation::new(SEED);

        let checker = rc!(refcell!(Checker::new(ExpectedEventType::WriteFailed)));
        let checker_id = sim.add_handler("User", checker);

        let fs = make_filesystem(&mut sim, "FS-1");
        let disk = make_disk(&mut sim, "Disk-1");

        assert!(fs.borrow_mut().mount_disk("/mnt", disk).is_ok());
        assert!(fs.borrow_mut().create_file("/mnt/file").is_ok());

        fs.borrow_mut().write("/mnt/file", 101, checker_id);
        sim.step_until_no_events();
    }
}

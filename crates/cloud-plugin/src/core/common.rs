use serde::Serialize;
use std::fmt::{Display, Formatter, Result};

#[derive(PartialEq)]
pub enum AllocationVerdict {
    NotEnoughCPU,
    NotEnoughMemory,
    Success,
    HostNotFound,
}

#[derive(Clone, PartialEq, Serialize)]
pub enum VmStatus {
    Initializing,
    Running,
    Deactivated,
    Migrating,
}

impl Display for VmStatus {
    fn fmt(&self, f: &mut Formatter) -> Result {
        match self {
            VmStatus::Initializing => write!(f, "initializing"),
            VmStatus::Running => write!(f, "running"),
            VmStatus::Deactivated => write!(f, "deactivated"),
            VmStatus::Migrating => write!(f, "migrating"),
        }
    }
}

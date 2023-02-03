//! Standard simulation events.

// VM ALLOCATION EVENTS ////////////////////////////////////////////////////////////////////////////

pub mod allocation {
    use serde::Serialize;

    #[derive(Serialize)]
    pub struct AllocationRequest {
        pub vm_ids: Vec<u32>,
    }

    #[derive(Serialize)]
    pub struct AllocationCommitRequest {
        pub vm_ids: Vec<u32>,
        pub host_ids: Vec<u32>,
    }

    #[derive(Serialize)]
    pub struct AllocationCommitSucceeded {
        pub vm_ids: Vec<u32>,
        pub host_ids: Vec<u32>,
    }

    #[derive(Serialize)]
    pub struct AllocationCommitFailed {
        pub vm_ids: Vec<u32>,
        pub host_ids: Vec<u32>,
    }

    #[derive(Serialize)]
    pub struct AllocationFailed {
        pub vm_id: u32,
        pub host_id: u32,
    }

    #[derive(Serialize)]
    pub struct AllocationReleased {
        pub vm_id: u32,
        pub host_id: u32,
    }

    #[derive(Serialize)]
    pub struct VmCreateRequest {
        pub vm_id: u32,
    }

    #[derive(Serialize, Clone)]
    pub struct AllocationReleaseRequest {
        pub vm_id: u32,
        pub is_migrating: bool,
    }

    #[derive(Serialize)]
    pub struct MigrationRequest {
        pub source_host: u32,
        pub vm_id: u32,
    }
}

// VM LIFECYCLE EVENTS /////////////////////////////////////////////////////////////////////////////

pub mod vm {
    use serde::Serialize;

    #[derive(Serialize)]
    pub struct VMStarted {
        pub vm_id: u32,
    }

    #[derive(Serialize)]
    pub struct VMDeleted {
        pub vm_id: u32,
    }
}

// MONITORING EVENTS ///////////////////////////////////////////////////////////////////////////////

pub mod monitoring {
    use serde::Serialize;

    #[derive(Serialize)]
    pub struct HostStateUpdate {
        pub host_id: u32,
        pub cpu_load: f64,
        pub memory_load: f64,
        pub recently_added_vms: Vec<u32>,
        pub recently_removed_vms: Vec<u32>,
    }
}

pub mod vm_api {
    use serde::Serialize;

    use crate::core::vm::VmStatus;
    #[derive(Serialize)]
    pub struct VmStatusChanged {
        pub vm_id: u32,
        pub status: VmStatus,
    }
}

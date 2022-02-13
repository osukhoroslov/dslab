// VM ALLOCATION EVENTS ////////////////////////////////////////////////////////////////////////////

pub mod allocation {
    use crate::vm::VirtualMachine;

    #[derive(Debug, Clone)]
    pub struct AllocationRequest {
        pub vm: VirtualMachine,
    }

    #[derive(Debug)]
    pub struct AllocationCommitRequest {
        pub vm: VirtualMachine,
        pub host_id: String,
    }

    #[derive(Debug, Clone)]
    pub struct AllocationCommitSucceeded {
        pub vm: VirtualMachine,
        pub host_id: String,
    }

    #[derive(Debug, Clone)]
    pub struct AllocationCommitFailed {
        pub vm: VirtualMachine,
        pub host_id: String,
    }

    #[derive(Debug)]
    pub struct AllocationFailed {
        pub vm: VirtualMachine,
        pub host_id: String,
    }

    #[derive(Debug)]
    pub struct AllocationReleased {
        pub vm: VirtualMachine,
        pub host_id: String,
    }

    #[derive(Debug, Clone)]
    pub struct AllocationReleaseRequest {
        pub vm: VirtualMachine,
    }
}

// VM LIFECYCLE EVENTS /////////////////////////////////////////////////////////////////////////////

pub mod vm {
    #[derive(Debug)]
    pub struct VMStartRequest {
        pub host_id: String,
    }

    #[derive(Debug)]
    pub struct VMStarted {
        pub vm_id: String,
    }

    #[derive(Debug)]
    pub struct VMDeleteRequest {}

    #[derive(Debug)]
    pub struct VMDeleted {
        pub vm_id: String,
    }
}

// MONITORING EVENTS ///////////////////////////////////////////////////////////////////////////////

pub mod monitoring {
    #[derive(Debug, Clone)]
    pub struct HostStateUpdate {
        pub host_id: String,
        pub cpu_load: f64,
        pub memory_load: f64,
    }
}

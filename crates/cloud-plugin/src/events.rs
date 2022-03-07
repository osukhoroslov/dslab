// VM ALLOCATION EVENTS ////////////////////////////////////////////////////////////////////////////

pub mod allocation {
    use crate::resource_pool::Allocation;
    use crate::vm::VirtualMachine;

    #[derive(Debug)]
    pub struct AllocationRequest {
        pub alloc: Allocation,
        pub vm: VirtualMachine,
    }

    #[derive(Debug)]
    pub struct AllocationCommitRequest {
        pub alloc: Allocation,
        pub vm: VirtualMachine,
        pub host_id: String,
    }

    #[derive(Debug)]
    pub struct AllocationCommitSucceeded {
        pub alloc: Allocation,
        pub host_id: String,
    }

    #[derive(Debug)]
    pub struct AllocationCommitFailed {
        pub alloc: Allocation,
        pub host_id: String,
    }

    #[derive(Debug)]
    pub struct AllocationFailed {
        pub alloc: Allocation,
        pub host_id: String,
    }

    #[derive(Debug)]
    pub struct AllocationReleased {
        pub alloc: Allocation,
        pub host_id: String,
    }

    #[derive(Debug, Clone)]
    pub struct AllocationReleaseRequest {
        pub alloc: Allocation,
    }
}

// VM LIFECYCLE EVENTS /////////////////////////////////////////////////////////////////////////////

pub mod vm {
    use crate::resource_pool::Allocation;

    #[derive(Debug)]
    pub struct VMStarted {
        pub alloc: Allocation,
    }

    #[derive(Debug)]
    pub struct VMDeleted {
        pub alloc: Allocation,
    }
}

// MONITORING EVENTS ///////////////////////////////////////////////////////////////////////////////

pub mod monitoring {
    #[derive(Debug)]
    pub struct HostStateUpdate {
        pub host_id: String,
        pub cpu_load: f64,
        pub memory_load: f64,
    }
}

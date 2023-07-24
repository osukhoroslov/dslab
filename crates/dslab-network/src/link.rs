//! Network link.

/// Unique link id.
pub type LinkId = usize;

/// Defines how the link bandwidth is shared among concurrent data transfers.
#[derive(Copy, Clone, Debug)]
pub enum BandwidthSharingPolicy {
    /// The bandwidth is shared equally between all transfers.
    Shared,
    /// Each transfer gets the full link bandwidth.
    NonShared,
}

/// A link between two nodes in the network.
#[derive(Copy, Clone, Debug)]
pub struct Link {
    /// Link bandwidth.
    pub bandwidth: f64,
    /// Link latency.
    pub latency: f64,
    /// Used bandwidth sharing policy.
    pub sharing_policy: BandwidthSharingPolicy,
}

impl Link {
    /// Creates a new link with [`BandwidthSharingPolicy::Shared`] policy.
    pub fn shared(bandwidth: f64, latency: f64) -> Self {
        Self {
            bandwidth,
            latency,
            sharing_policy: BandwidthSharingPolicy::Shared,
        }
    }

    /// Creates a new link with [`BandwidthSharingPolicy::NonShared`] policy.
    pub fn non_shared(bandwidth: f64, latency: f64) -> Self {
        Self {
            bandwidth,
            latency,
            sharing_policy: BandwidthSharingPolicy::NonShared,
        }
    }
}

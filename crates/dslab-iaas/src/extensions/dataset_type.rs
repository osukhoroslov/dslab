//! VM dataset types supported by the crate.

use std::str::FromStr;

use log::warn;
use serde::{Deserialize, Serialize};

/// Represents VM dataset types supported by framework
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum VmDatasetType {
    Azure,
    Huawei,
}

impl FromStr for VmDatasetType {
    type Err = ();
    fn from_str(input: &str) -> Result<VmDatasetType, Self::Err> {
        if input != "azure" && input != "huawei" {
            warn!("Cannot parse dataset type, use azure as default");
        }
        match input {
            "azure" => Ok(VmDatasetType::Azure),
            "huawei" => Ok(VmDatasetType::Huawei),
            _ => Ok(VmDatasetType::Azure),
        }
    }
}

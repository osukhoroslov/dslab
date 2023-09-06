//! VM dataset types.

use std::str::FromStr;

use log::warn;
use serde::{Deserialize, Serialize};

/// Holds supported VM dataset types.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum VmDatasetType {
    Azure,
    Huawei,
}

impl FromStr for VmDatasetType {
    type Err = ();
    fn from_str(input: &str) -> Result<VmDatasetType, Self::Err> {
        if input != "azure" && input != "huawei" {
            warn!("Cannot parse dataset type, will use azure as default");
        }
        match input {
            "azure" => Ok(VmDatasetType::Azure),
            "huawei" => Ok(VmDatasetType::Huawei),
            _ => Ok(VmDatasetType::Azure),
        }
    }
}

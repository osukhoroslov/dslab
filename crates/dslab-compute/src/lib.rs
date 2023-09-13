//! A library for modeling computations.
//!
//! ## Examples
//!
//! - [compute-singlecore](https://github.com/osukhoroslov/dslab/tree/main/examples/compute-singlecore):
//! demonstrates the use of an [singlecore::Compute] actor.
//! - [compute-multicore](https://github.com/osukhoroslov/dslab/tree/main/examples/compute-multicore):
//! demonstrates the use of an [multicore::Compute] actor.

#![warn(missing_docs)]

pub mod multicore;
pub mod singlecore;

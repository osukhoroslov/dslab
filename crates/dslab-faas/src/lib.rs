//! A library for studying resource management in FaaS platforms.
//!
//! ## Examples
//!
//! - [faas](https://github.com/osukhoroslov/dslab/tree/main/examples/faas): demonstrates basic usage of DSLab FaaS.
//! - [faas-parallel](https://github.com/osukhoroslov/dslab/tree/main/examples/faas-parallel): demonstrates running
//! parallel experiments with DSLab FaaS.
//!
//! Also refer to [serverless-in-the-wild](https://github.com/osukhoroslov/dslab/tree/main/examples/serverless-in-the-wild) and [faas-scheduling-experiment](https://github.com/osukhoroslov/dslab/tree/main/examples/faas-scheduling-experiment), which reproduce experiments done in real research.

#![allow(clippy::type_complexity)]

pub mod coldstart;
pub mod config;
pub mod container;
pub mod controller;
pub mod cpu;
pub mod deployer;
pub mod event;
pub mod extra;
pub mod function;
pub mod host;
pub mod invocation;
pub mod invoker;
pub mod parallel;
pub mod resource;
pub mod scheduler;
pub mod simulation;
pub mod stats;
pub mod trace;
pub mod util;

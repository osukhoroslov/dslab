use colored::*;

use crate::mc::events::McEvent;
use crate::mc::events::McEvent::{MessageReceived, TimerFired};
use crate::mc::system::McSystem;
use crate::util::t;

pub trait Strategy {
    fn run(&mut self, system: &mut McSystem) -> bool;

    fn debug_log(event: &McEvent, depth: u64)
    where
        Self: Sized,
    {
        match event {
            MessageReceived { msg, src, dest } => {
                t!("{:>10} | {:>10} <-- {:<10} {:?}", depth, dest, src, msg);
            }
            TimerFired { proc, timer } => {
                t!(format!("{:>10} | {:>10} !-- {:<10}", depth, proc, timer).yellow());
            }
        }
    }
}

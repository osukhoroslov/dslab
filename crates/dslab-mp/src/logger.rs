use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::Path,
};

use colored::Colorize;
use dslab_core::Id;
use serde::Serialize;

use crate::{message::Message, util::t};

pub struct Logger {
    log_file: Option<File>,
    trace: Vec<LogEntry>,
}

impl Logger {
    pub fn new() -> Self {
        Self {
            log_file: None,
            trace: vec![],
        }
    }

    pub fn with_log_file(log_path: &Path) -> Self {
        let log_file = Some(
            OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(log_path)
                .unwrap(),
        );
        Self {
            log_file,
            trace: vec![],
        }
    }

    pub fn has_log_file(&self) -> bool {
        self.log_file.is_some()
    }

    pub fn log(&mut self, event: LogEntry) {
        if let Some(log_file) = self.log_file.as_mut() {
            let serialized = serde_json::to_string(&event).unwrap();
            log_file.write_all(serialized.as_bytes()).unwrap();
            log_file.write_all("\n".as_bytes()).unwrap();
        }

        self.trace.push(event.clone());

        event.print();
    }

    pub fn trace(&self) -> &Vec<LogEntry> {
        &self.trace
    }
}

impl Default for Logger {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum LogEntry {
    NodeStarted {
        time: f64,
        node: String,
        node_id: Id,
    },
    ProcessStarted {
        time: f64,
        node: String,
        proc: String,
    },
    LocalMessageSent {
        time: f64,
        msg_id: String,
        node: String,
        proc: String,
        msg: Message,
    },
    LocalMessageReceived {
        time: f64,
        msg_id: String,
        node: String,
        proc: String,
        msg: Message,
    },
    MessageSent {
        time: f64,
        msg_id: String,
        src_node: String,
        src_proc: String,
        dest_node: String,
        dest_proc: String,
        msg: Message,
    },
    MessageReceived {
        time: f64,
        msg_id: String,
        #[serde(skip_serializing)]
        src_proc: String,
        #[serde(skip_serializing)]
        src_node: String,
        #[serde(skip_serializing)]
        dest_proc: String,
        #[serde(skip_serializing)]
        dest_node: String,
        #[serde(skip_serializing)]
        msg: Message,
    },
    MessageDropped {
        time: f64,
        msg_id: String,
        #[serde(skip_serializing)]
        src_proc: String,
        #[serde(skip_serializing)]
        src_node: String,
        #[serde(skip_serializing)]
        dest_proc: String,
        #[serde(skip_serializing)]
        dest_node: String,
        #[serde(skip_serializing)]
        msg: Message,
    },
    NodeDisconnected {
        time: f64,
        node: String,
    },
    NodeConnected {
        time: f64,
        node: String,
    },
    NodeCrashed {
        time: f64,
        node: String,
    },
    NodeRecovered {
        time: f64,
        node: String,
    },
    TimerSet {
        time: f64,
        timer_id: String,
        timer_name: String,
        node: String,
        proc: String,
        delay: f64,
    },
    TimerFired {
        time: f64,
        timer_id: String,
        #[serde(skip_serializing)]
        timer_name: String,
        #[serde(skip_serializing)]
        node: String,
        #[serde(skip_serializing)]
        proc: String,
    },
    TimerCancelled {
        time: f64,
        timer_id: String,
        #[serde(skip_serializing)]
        timer_name: String,
        #[serde(skip_serializing)]
        node: String,
        #[serde(skip_serializing)]
        proc: String,
    },
    LinkDisabled {
        time: f64,
        from: String,
        to: String,
    },
    LinkEnabled {
        time: f64,
        from: String,
        to: String,
    },
    DropIncoming {
        time: f64,
        node: String,
    },
    PassIncoming {
        time: f64,
        node: String,
    },
    DropOutgoing {
        time: f64,
        node: String,
    },
    PassOutgoing {
        time: f64,
        node: String,
    },
    NetworkPartition {
        time: f64,
        group1: Vec<String>,
        group2: Vec<String>,
    },
    NetworkReset {
        time: f64,
    },
    ProcessStateUpdated {
        time: f64,
        node: String,
        proc: String,
        state: String,
    },
    McStarted {},
    McLocalMessageSent {
        msg: Message,
        proc: String,
    },
    McLocalMessageReceived {
        msg: Message,
        proc: String,
    },
    McMessageSent {
        msg: Message,
        src: String,
        dest: String,
    },
    McMessageReceived {
        msg: Message,
        src: String,
        dest: String,
    },
    McMessageDropped {
        msg: Message,
        src: String,
        dest: String,
    },
    McMessageCorrupted {
        msg: Message,
        corrupted_msg: Message,
        src: String,
        dest: String,
    },
    McMessageDuplicated {
        msg: Message,
        src: String,
        dest: String,
    },
    McTimerSet {
        proc: String,
        timer: String,
    },
    McTimerFired {
        proc: String,
        timer: String,
    },
    McTimerCancelled {
        proc: String,
        timer: String,
    },
}

impl LogEntry {
    pub fn print(&self) {
        match self {
            LogEntry::NodeStarted { time, node, .. } => {
                t!(format!("{:>9.3} - node started: {}", time, node));
            }
            LogEntry::ProcessStarted { time, node, proc } => {
                t!(format!("{:>9.3} - process started: {} @ {}", time, proc, node));
            }
            LogEntry::LocalMessageSent {
                time,
                msg_id: _,
                node: _,
                proc,
                msg,
            } => {
                t!(format!("{:>9.3} {:>10} >>> {:<10} {:?}", time, proc, "local", msg).green());
            }
            LogEntry::LocalMessageReceived {
                time,
                msg_id: _,
                node: _,
                proc,
                msg,
            } => {
                t!(format!("{:>9.3} {:>10} <<< {:<10} {:?}", time, proc, "local", msg).cyan());
            }
            LogEntry::MessageSent {
                time,
                msg_id: _,
                src_node: _,
                src_proc,
                dest_node: _,
                dest_proc,
                msg,
            } => {
                t!(format!(
                    "{:>9.3} {:>10} --> {:<10} {:?}",
                    time, src_proc, dest_proc, msg
                ));
            }
            LogEntry::MessageReceived {
                time,
                msg_id: _,
                src_proc,
                src_node: _,
                dest_proc,
                dest_node: _,
                msg,
            } => {
                t!(format!(
                    "{:>9.3} {:>10} <-- {:<10} {:?}",
                    time, dest_proc, src_proc, msg
                ))
            }
            LogEntry::MessageDropped {
                time: _,
                msg_id: _,
                src_proc,
                src_node: _,
                dest_proc,
                dest_node: _,
                msg,
            } => {
                t!(format!(
                    "{:>9} {:>10} --x {:<10} {:?} <-- message dropped",
                    "!!!", src_proc, dest_proc, msg
                )
                .red());
            }
            LogEntry::NodeConnected { time, node } => {
                t!(format!("{:>9.3} - connected node: {}", time, node).green());
            }
            LogEntry::NodeDisconnected { time, node } => {
                t!(format!("{:>9.3} - disconnected node: {}", time, node).red());
            }
            LogEntry::NodeCrashed { time, node } => {
                t!(format!("{:>9.3} - node crashed: {}", time, node).red());
            }
            LogEntry::NodeRecovered { time, node } => {
                t!(format!("{:>9.3} - node recovered: {}", time, node).green());
            }
            LogEntry::TimerSet { .. } => {}
            LogEntry::TimerFired {
                time,
                timer_id: _,
                timer_name,
                node: _,
                proc,
            } => {
                t!(format!("{:>9.3} {:>10} !-- {:<10}", time, proc, timer_name).yellow());
            }
            LogEntry::TimerCancelled { .. } => {}
            LogEntry::LinkDisabled { time, from, to } => {
                t!(format!("{:>9.3} - disabled link: {:>10} --> {:<10}", time, from, to).red());
            }
            LogEntry::LinkEnabled { time, from, to } => {
                t!(format!("{:>9.3} - enabled link: {:>10} --> {:<10}", time, from, to).green());
            }
            LogEntry::DropIncoming { time, node } => {
                t!(format!("{:>9.3} - drop messages to {}", time, node).red());
            }
            LogEntry::PassIncoming { time, node } => {
                t!(format!("{:>9.3} - pass messages to {}", time, node).green());
            }
            LogEntry::DropOutgoing { time, node } => {
                t!(format!("{:>9.3} - drop messages from {}", time, node).red());
            }
            LogEntry::PassOutgoing { time, node } => {
                t!(format!("{:>9.3} - pass messages from {}", time, node).green());
            }
            LogEntry::NetworkPartition { time, group1, group2 } => {
                t!(format!("{:>9.3} - network partition: {:?} -x- {:?}", time, group1, group2).red());
            }
            LogEntry::NetworkReset { time } => {
                t!(format!("{:>9.3} - network reset, all problems healed", time).green());
            }
            LogEntry::ProcessStateUpdated { .. } => {}
            LogEntry::McStarted { .. } => {
                t!("MODEL CHECKING STARTED");
            }
            LogEntry::McLocalMessageSent { msg, proc } => {
                t!(format!("{:>10} >>> {:<10} {:?}", proc, "local", msg).green());
            }
            LogEntry::McLocalMessageReceived { msg, proc } => {
                t!(format!("{:>10} <<< {:<10} {:?}", "local", proc, msg).cyan());
            }
            LogEntry::McMessageSent { msg, src, dest } => {
                t!(format!("{:>10} --> {:<10} {:?}", src, dest, msg));
            }
            LogEntry::McMessageReceived { msg, src, dest } => {
                t!("{:>10} <-- {:<10} {:?}", dest, src, msg);
            }
            LogEntry::McMessageDropped { msg, src, dest } => {
                t!(format!("{:>10} --x {:<10} {:?} <-- message dropped", src, dest, msg).red());
            }
            LogEntry::McMessageCorrupted {
                msg,
                corrupted_msg,
                src,
                dest,
            } => {
                t!(format!(
                    "{:>10} -x- {:<10} {:?} ~~> {:?} <-- message corrupted",
                    src, dest, msg, corrupted_msg
                )
                .blue());
            }
            LogEntry::McMessageDuplicated { msg, src, dest } => {
                t!(format!(
                    "{:>9} {:>10} -=≡ {:<10} {:?} <-- message duplicated",
                    "~~~", src, dest, msg
                )
                .blue());
            }
            LogEntry::McTimerSet { proc, timer } => {
                t!(format!("{:>10} +++ {:<10} <-- timer set", proc, timer));
            }
            LogEntry::McTimerFired { proc, timer } => {
                t!(format!("{:>10} !-- {:<10} <-- timer fired", proc, timer).yellow());
            }
            LogEntry::McTimerCancelled { proc, timer } => {
                t!(format!("{:>10} xxx {:<10} <-- timer cancelled", proc, timer).yellow());
            }
        }
    }
}

use macroquad::prelude::*;
use macroquad::rand::gen_range;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::{collections::HashMap, f32::consts::PI};

use crate::logs::log_entities::*;

use super::animation::state::State;
use super::utilities::{prettify_json_string, CIRCLE_RADIUS};

#[derive(Debug)]
pub enum LogEventType {
    MessageSent(String),
    LocalMessageEmerged(String),
    NodeConnected(String),
    NodeDisconnected(String),
    NodeStarted(ProcessorNode),
    TimerSet(String),
    LinkDisabled((String, String)),
    LinkEnabled((String, String)),
    DropIncoming(String),
    PassIncoming(String),
    DropOutgoing(String),
    PassOutgoing(String),
    NetworkPartition((Vec<String>, Vec<String>)),
    NetworkReset(),
    NodeStateUpdated((String, String)),
}

pub struct LogProcessor {
    local_messages: HashMap<String, ProcessorLocalMessage>,
    messages: HashMap<String, ProcessorMessage>,
    timers: HashMap<String, ProcessorTimer>,
    commands: Vec<(f64, LogEventType)>,
}

impl LogProcessor {
    pub fn new() -> Self {
        Self {
            local_messages: HashMap::new(),
            messages: HashMap::new(),
            timers: HashMap::new(),
            commands: vec![],
        }
    }

    pub fn parse_log(&mut self, filename: &str) {
        let mut events: Vec<LogEntry> = Vec::new();

        let file = File::open(filename).unwrap();
        let reader = BufReader::new(file);

        for line in reader.lines() {
            let event: LogEntry = serde_json::from_str(&line.unwrap()).unwrap();
            events.push(event);
        }

        let mut node_cnt = 0;
        let mut process_cnt = 0;

        for event in &events {
            match event {
                LogEntry::NodeStarted { .. } => node_cnt += 1,
                LogEntry::ProcessStarted { .. } => process_cnt += 1,
                _ => break,
            }
        }

        let center = Vec2::new(screen_width() / 2., screen_height() / 2.);
        let mut k = 0;
        for event in events.iter().take(node_cnt + process_cnt) {
            let angle = (2.0 * PI / (node_cnt as f32)) * (k as f32);
            let pos = center + Vec2::from_angle(angle) * CIRCLE_RADIUS;
            if let LogEntry::NodeStarted { time, node, node_id } = &event {
                self.commands.push((
                    *time,
                    LogEventType::NodeStarted(ProcessorNode {
                        name: node.clone(),
                        id: *node_id,
                        pos,
                    }),
                ));
                k += 1;
            }
        }

        for event in events.split_off(node_cnt + process_cnt) {
            match event {
                LogEntry::NodeStarted { time, node, node_id } => {
                    let x = gen_range(0.3, 0.8);
                    let y = gen_range(0.3, 0.8);
                    let pos = Vec2::from((x * screen_height(), y * screen_width()));
                    self.commands.push((
                        time,
                        LogEventType::NodeStarted(ProcessorNode {
                            name: node,
                            id: node_id,
                            pos,
                        }),
                    ));
                }
                LogEntry::ProcessStarted { .. } => {}
                LogEntry::LocalMessageSent {
                    time,
                    msg_id,
                    node,
                    proc,
                    msg,
                } => {
                    let processor_msg = ProcessorLocalMessage {
                        id: msg_id.clone(),
                        node,
                        proc,
                        tip: msg.tip,
                        data: prettify_json_string(msg.data),
                        time,
                        msg_type: LocalMessageType::Sent,
                    };
                    self.local_messages.insert(msg_id.clone(), processor_msg);
                    self.commands
                        .push((time, LogEventType::LocalMessageEmerged(msg_id.clone())));
                }
                LogEntry::LocalMessageReceived {
                    time,
                    msg_id,
                    node,
                    proc,
                    msg,
                } => {
                    let processor_msg = ProcessorLocalMessage {
                        id: msg_id.clone(),
                        node,
                        proc,
                        tip: msg.tip,
                        data: msg.data,
                        time,
                        msg_type: LocalMessageType::Received,
                    };
                    self.local_messages.insert(msg_id.clone(), processor_msg);
                    self.commands
                        .push((time, LogEventType::LocalMessageEmerged(msg_id.clone())));
                }
                LogEntry::MessageSent {
                    time,
                    msg_id,
                    src_node,
                    src_proc,
                    dest_node,
                    dest_proc,
                    msg,
                } => {
                    let cont_msg = ProcessorMessage {
                        id: msg_id.clone(),
                        src_node,
                        src_proc,
                        dest_node,
                        dest_proc,
                        tip: msg.tip,
                        data: msg.data,
                        time_sent: time,
                        times_received: Vec::new(),
                        copies_received: 0,
                    };
                    self.messages.insert(cont_msg.id.clone(), cont_msg);
                    self.commands.push((time, LogEventType::MessageSent(msg_id)));
                }
                LogEntry::MessageReceived { time, msg_id } => {
                    let msg = self.messages.get_mut(&msg_id).unwrap();
                    msg.times_received.push(time);
                    msg.copies_received += 1;
                }
                LogEntry::MessageDropped { .. } => {}
                LogEntry::NodeDisconnected { time, node } => {
                    self.commands.push((time, LogEventType::NodeDisconnected(node)));
                }
                LogEntry::NodeConnected { time, node } => {
                    self.commands.push((time, LogEventType::NodeConnected(node)));
                }
                LogEntry::NodeCrashed { time, node } => {
                    self.commands.push((time, LogEventType::NodeDisconnected(node)));
                }
                LogEntry::NodeRecovered { time, node } => {
                    self.commands.push((time, LogEventType::NodeConnected(node)));
                }
                LogEntry::TimerSet {
                    time,
                    timer_id,
                    timer_name,
                    node,
                    proc,
                    delay,
                } => {
                    let timer = ProcessorTimer {
                        id: timer_id.clone(),
                        name: timer_name,
                        node,
                        proc,
                        delay,
                        time_set: time,
                        time_removed: -1.,
                    };
                    self.timers.insert(timer_id.clone(), timer);
                    self.commands.push((time, LogEventType::TimerSet(timer_id)));
                }
                LogEntry::TimerFired { time, timer_id } => {
                    self.timers.get_mut(&timer_id).unwrap().time_removed = time;
                }
                LogEntry::TimerCancelled { time, timer_id } => {
                    self.timers.get_mut(&timer_id).unwrap().time_removed = time;
                }
                LogEntry::LinkDisabled { time, from, to } => {
                    self.commands.push((time, LogEventType::LinkDisabled((from, to))));
                }
                LogEntry::LinkEnabled { time, from, to } => {
                    self.commands.push((time, LogEventType::LinkEnabled((from, to))));
                }
                LogEntry::DropIncoming { time, node } => {
                    self.commands.push((time, LogEventType::DropIncoming(node)));
                }
                LogEntry::PassIncoming { time, node } => {
                    self.commands.push((time, LogEventType::PassIncoming(node)));
                }
                LogEntry::DropOutgoing { time, node } => {
                    self.commands.push((time, LogEventType::DropOutgoing(node)));
                }
                LogEntry::PassOutgoing { time, node } => {
                    self.commands.push((time, LogEventType::PassOutgoing(node)));
                }
                LogEntry::NetworkPartition { time, group1, group2 } => {
                    self.commands
                        .push((time, LogEventType::NetworkPartition((group1, group2))));
                }
                LogEntry::NetworkReset { time } => {
                    self.commands.push((time, LogEventType::NetworkReset()));
                }
                LogEntry::ProcessStateUpdated {
                    time,
                    node,
                    proc: _,
                    state,
                } => {
                    let pretty_state = prettify_json_string(state).replace('\\', "");
                    self.commands
                        .push((time, LogEventType::NodeStateUpdated((node, pretty_state))));
                }
            }
        }
    }

    pub fn send_commands(&mut self, state: &mut State) {
        self.commands.sort_by(|a, b| a.0.total_cmp(&b.0));
        for command in &self.commands {
            match &command.1 {
                LogEventType::NodeStarted(node) => {
                    state.process_node_started(command.0, node.name.clone(), node.id, node.pos);
                }
                LogEventType::MessageSent(id) => {
                    let msg = self.messages.get(id).unwrap();
                    for i in 0..msg.copies_received {
                        state.process_message_sent(
                            format!("{}-{}", msg.id, i),
                            msg.time_sent,
                            &msg.src_node,
                            &msg.dest_node,
                            msg.tip.clone(),
                            prettify_json_string(msg.data.clone()),
                            (msg.times_received[i as usize] - msg.time_sent) as f32,
                            msg.copies_received,
                        );
                    }
                }
                LogEventType::LocalMessageEmerged(id) => {
                    let msg = self.local_messages.get(id).unwrap();
                    let is_sent = match msg.msg_type {
                        LocalMessageType::Received => false,
                        LocalMessageType::Sent => true,
                    };
                    state.process_local_message(
                        msg.time,
                        msg.id.clone(),
                        msg.node.clone(),
                        msg.tip.clone(),
                        msg.data.clone(),
                        is_sent,
                    );
                }
                LogEventType::NodeDisconnected(node) => state.process_node_disconnected(command.0, node.clone()),
                LogEventType::NodeConnected(node) => state.process_node_connected(command.0, node.clone()),
                LogEventType::TimerSet(id) => {
                    let timer = self.timers.get(id).unwrap();
                    state.process_timer_set(
                        timer.id.clone(),
                        timer.name.clone(),
                        timer.time_set,
                        timer.node.clone(),
                        timer.delay,
                        timer.time_removed,
                    );
                }
                LogEventType::LinkDisabled(link) => {
                    state.process_link_disabled(command.0, link.0.clone(), link.1.clone());
                }
                LogEventType::LinkEnabled(link) => {
                    state.process_link_enabled(command.0, link.0.clone(), link.1.clone());
                }
                LogEventType::DropIncoming(node) => {
                    state.process_drop_incoming(command.0, node.clone());
                }
                LogEventType::PassIncoming(node) => {
                    state.process_pass_incoming(command.0, node.clone());
                }
                LogEventType::DropOutgoing(node) => {
                    state.process_drop_outgoing(command.0, node.clone());
                }
                LogEventType::PassOutgoing(node) => {
                    state.process_pass_outgoing(command.0, node.clone());
                }
                LogEventType::NetworkPartition((group1, group2)) => {
                    state.process_network_partition(command.0, group1.clone(), group2.clone());
                }
                LogEventType::NetworkReset() => {
                    state.process_network_reset(command.0);
                }
                LogEventType::NodeStateUpdated((node, process_state)) => {
                    state.process_state_updated(command.0, node.to_string(), process_state.to_string());
                }
            }
        }
    }
}

pub enum LocalMessageType {
    Sent,
    Received,
}

pub struct ProcessorLocalMessage {
    id: String,
    node: String,
    #[allow(dead_code)]
    proc: String,
    #[allow(dead_code)]
    tip: String,
    data: String,
    time: f64,
    msg_type: LocalMessageType,
}

pub struct ProcessorMessage {
    id: String,
    src_node: String,
    #[allow(dead_code)]
    src_proc: String,
    dest_node: String,
    #[allow(dead_code)]
    dest_proc: String,
    tip: String,
    data: String,
    time_sent: f64,
    times_received: Vec<f64>,
    copies_received: u64,
}

pub struct ProcessorTimer {
    id: String,
    name: String,
    node: String,
    #[allow(dead_code)]
    proc: String,
    time_set: f64,
    delay: f64,
    time_removed: f64,
}

#[derive(Debug)]
pub struct ProcessorNode {
    name: String,
    id: u32,
    pos: Vec2,
}

use std::{cell::RefCell, rc::Rc};

use crate::visualization::utilities::*;
use egui::Context;
use macroquad::prelude::*;

use super::{node::*, state::State};

#[derive(Debug, Clone)]
pub struct StateMessage {
    pub id: String,
    pub relative_pos: Vec2,
    pub src: Rc<RefCell<StateNode>>,
    pub dest: Rc<RefCell<StateNode>>,
    pub tip: String,
    pub data: String,
    pub status: MessageStatus,
    pub time_sent: f32,
    pub time_delivered: f32,
    pub copies_received: u64,
    pub last_color_change: f64,
    pub color: Color,
}

impl StateMessage {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: String,
        src: Rc<RefCell<StateNode>>,
        dest: Rc<RefCell<StateNode>>,
        tip: String,
        data: String,
        status: MessageStatus,
        time_sent: f32,
        time_delivered: f32,
        copies_received: u64,
    ) -> Self {
        let relative_pos = get_relative_pos(src.borrow().get_pos());
        let color = src.borrow().color;
        Self {
            id,
            relative_pos,
            src,
            dest,
            tip,
            data,
            status,
            time_sent,
            time_delivered,
            copies_received,
            last_color_change: 0.,
            color,
        }
    }

    pub fn update_pos(&mut self, new_pos: Vec2) {
        self.relative_pos = get_relative_pos(new_pos);
    }

    pub fn get_pos(&self) -> Vec2 {
        get_absolute_pos(self.relative_pos)
    }

    pub fn get_direction(&self) -> Vec2 {
        self.dest.borrow().get_pos() - self.get_pos()
    }

    pub fn get_own_speed(&self, current_time: f32, last_msg_speed: f32) -> f32 {
        let direction = self.get_direction();
        let travel_time_left = self.time_delivered - current_time;
        let mut own_speed = if !self.is_dropped() {
            1.0 / ((get_fps() as f32) * travel_time_left / direction.length())
        } else {
            1.0 / ((get_fps() as f32) * last_msg_speed)
        };
        if own_speed < 0. {
            own_speed = MAX_MESSAGE_SPEED;
        }
        own_speed
    }

    pub fn update(&mut self, global_speed: f32, current_time: f32, last_msg_speed: f32) {
        let direction = self.get_direction();
        let own_speed = self.get_own_speed(current_time, last_msg_speed);

        let mut new_pos = self.get_pos() + direction.normalize() * own_speed * global_speed;

        if (new_pos - self.get_pos()).length() > (self.dest.borrow().get_pos() - self.get_pos()).length() {
            new_pos = self.dest.borrow().get_pos();
        }

        self.update_pos(new_pos);

        let time = get_time();
        if self.is_dropped() && time - self.last_color_change >= 0.3 {
            self.color = if self.color == BLACK {
                self.src.borrow().color
            } else {
                BLACK
            };
            self.last_color_change = time;
        }
        if !self.is_dropped() {
            self.color = self.src.borrow().color
        };
    }

    pub fn update_with_jump(&mut self, global_speed: f32, current_time: f32, delta: f32, last_msg_speed: f32) {
        let direction = self.get_direction();
        let own_speed = self.get_own_speed(current_time, last_msg_speed);
        let jump_dist = own_speed * global_speed * delta;
        let new_pos = if self.is_dropped() {
            self.dest.borrow().get_pos()
        } else {
            self.get_pos() + direction.normalize() * jump_dist
        };
        self.update_pos(new_pos);
    }

    pub fn draw(&self, state: &State) {
        let pos = self.get_pos();
        draw_circle(pos.x, pos.y, state.get_msg_radius(), self.color);
        if self.is_duplicated() {
            let font_size = (state.get_msg_radius() * 2.0).floor() as u16;
            let text = self.copies_received.to_string();
            let text_size = measure_text(&text, None, font_size, 1.0);
            let text_position = Vec2::new(pos.x - text_size.width / 2.0, pos.y + text_size.height / 2.0);

            draw_text_ex(
                &text,
                text_position.x,
                text_position.y,
                TextParams {
                    font_size,
                    color: BLACK,
                    ..Default::default()
                },
            );
        }
    }

    pub fn draw_ui(&self, egui_ctx: &Context, show_window: &mut bool) {
        egui::Window::new(format!("Message {}", self.id))
            .open(show_window)
            .show(egui_ctx, |ui| {
                ui.label(format!("Sent at: {:.7}", self.time_sent));
                ui.label(format!("From: {}", self.src.borrow().name));
                ui.label(format!("To: {}", self.dest.borrow().name));
                if self.is_duplicated() {
                    ui.label(format!("Duplicated {} times", self.copies_received));
                }
                ui.label(format!("Type: {}", self.tip));
                ui.label(format!("Data: {}", self.data.clone()));
            });
    }

    pub fn is_dropped(&self) -> bool {
        self.copies_received == 0
    }

    pub fn is_duplicated(&self) -> bool {
        self.copies_received > 1
    }

    pub fn is_delivered(&self, current_time: f32) -> bool {
        let pos = self.get_pos();
        if !self.is_dropped() {
            calc_dist(pos, self.dest.borrow().get_pos()) < 5.0 || current_time >= self.time_delivered
        } else {
            let overall_dist = calc_dist(self.src.borrow().get_pos(), self.dest.borrow().get_pos());
            calc_dist(self.src.borrow().get_pos(), pos) >= overall_dist * 0.25
        }
    }

    pub fn update_status(&mut self, current_time: f32) {
        if self.is_delivered(current_time) {
            self.status = if self.is_dropped() && self.src.borrow().id != self.dest.borrow().id {
                MessageStatus::Dropped
            } else {
                MessageStatus::Delivered
            };
        }
    }

    pub fn set_status(&mut self, status: MessageStatus) {
        self.status = status;
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MessageStatus {
    Queued,
    OnTheWay,
    Dropped,
    Delivered,
}

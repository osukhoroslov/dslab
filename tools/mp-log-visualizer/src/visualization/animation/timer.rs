use std::f32::consts::PI;

use egui::Context;
use macroquad::prelude::*;

use crate::visualization::utilities::*;

use super::state::State;

#[derive(Debug, Clone)]
pub struct StateTimer {
    pub id: String,
    pub name: String,
    pub time_set: f64,
    pub node: String,
    pub delay: f64,
    pub time_removed: f64,
    pub k: i32,
}

impl StateTimer {
    pub fn new(id: String, name: String, time_set: f64, node: String, delay: f64, time_removed: f64) -> Self {
        Self {
            id,
            name,
            time_set,
            node,
            delay,
            time_removed,
            k: -1,
        }
    }
    pub fn get_position(&self, node_pos: Vec2, node_radius: f32, timer_radius: f32) -> Vec2 {
        let angle = (2.0 * PI / (TIMERS_MAX_NUMBER as f32)) * (self.k as f32);
        node_pos + Vec2::from_angle(angle) * (node_radius + timer_radius + 5.)
    }

    pub fn check_hovered(&self, node_pos: Vec2, node_radius: f32, timer_radius: f32) -> bool {
        let mouse_pos = Vec2::from(mouse_position());
        calc_dist(self.get_position(node_pos, node_radius, timer_radius), mouse_pos) <= timer_radius
    }

    pub fn is_cancelled(&self) -> bool {
        self.time_removed != self.time_set + self.delay
    }

    pub fn draw(&self, node_pos: Vec2, state: &State) {
        let pos = self.get_position(node_pos, state.get_node_radius(), state.get_timer_radius());
        let mut color = TIMER_COLOR;
        let duration = state.current_time - self.time_set;
        if state.current_time >= self.time_removed * 0.95 {
            color = if self.time_removed < self.time_set + self.delay {
                CANCELLED_TIMER_COLOR
            } else {
                READY_TIMER_COLOR
            };
        }
        let end_angle = (duration * 2. * (PI as f64) / self.delay) as f32 - PI / 2.;
        draw_circle_segment(pos.x, pos.y, state.get_timer_radius(), -PI / 2., end_angle, color);
        draw_circle_lines(pos.x, pos.y, state.get_timer_radius(), 2., color)
    }

    pub fn draw_ui(&self, egui_ctx: &Context) {
        let default_pos = (screen_width() * 0.8, screen_height() * 0.8);
        egui::Window::new(format!("Timer {}", self.id))
            .default_pos(default_pos)
            .show(egui_ctx, |ui| {
                ui.label(format!("Name: {}", self.name));
                ui.label(format!("Timer delay: {}", self.delay));
                ui.label(format!("Time set: {:.7}", self.time_set));
                if self.is_cancelled() {
                    ui.label(format!("Time cancelled: {:.7}", self.time_removed));
                } else {
                    ui.label(format!("Time fired: {:.7}", self.time_removed));
                }
            });
    }
}

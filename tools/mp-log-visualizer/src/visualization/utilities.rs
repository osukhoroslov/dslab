use macroquad::prelude::*;
use serde_json::Value;

pub const DEFAULT_NODE_RADIUS: f32 = 15.;
pub const DEFAULT_MESSAGE_RADIUS: f32 = 5.;
pub const CIRCLE_RADIUS: f32 = 160.;
pub const DEFAULT_TIMER_RADIUS: f32 = 8.;
pub const PARTITIONED_CIRCLE_RADIUS: f32 = 100.;

pub const DEFAULT_NODE_COLOR: Color = YELLOW;
pub const DEAD_NODE_COLOR: Color = MAROON;

pub const TIMER_COLOR: Color = ORANGE;
pub const READY_TIMER_COLOR: Color = GREEN;
pub const CANCELLED_TIMER_COLOR: Color = RED;

pub const GLOBAL_SPEED_DELTA: f32 = 0.0002;
pub const SCALE_COEF_DELTA: f32 = 0.05;
pub const MAX_MESSAGE_SPEED: f32 = 30.;
pub const DEFAULT_GLOBAL_SPEED: f32 = 0.001;

pub const SINGLE_CLICK_DELAY: f64 = 0.12;

pub const TIMERS_MAX_NUMBER: usize = 9;

pub fn calc_dist(a: Vec2, b: Vec2) -> f32 {
    ((a.x - b.x) * (a.x - b.x) + (a.y - b.y) * (a.y - b.y)).sqrt()
}

pub fn get_relative_pos(pos: Vec2) -> Vec2 {
    Vec2 {
        x: pos.x / screen_width(),
        y: pos.y / screen_height(),
    }
}

pub fn get_absolute_pos(relative_pos: Vec2) -> Vec2 {
    Vec2 {
        x: relative_pos.x * screen_width(),
        y: relative_pos.y * screen_height(),
    }
}

pub fn prettify_json_string(str: String) -> String {
    let value: Value = serde_json::from_str(&str).unwrap();
    serde_json::to_string_pretty(&value).unwrap()
}

pub fn draw_circle_segment(x: f32, y: f32, r: f32, start_angle: f32, end_angle: f32, color: Color) {
    let num_segments = 100;
    let theta = (end_angle - start_angle) / num_segments as f32;
    for i in 0..num_segments {
        let angle_start = start_angle + theta * i as f32;
        let angle_end = start_angle + theta * (i + 1) as f32;
        let start_x = x + r * angle_start.cos();
        let start_y = y + r * angle_start.sin();
        let end_x = x + r * angle_end.cos();
        let end_y = y + r * angle_end.sin();
        draw_line(start_x, start_y, end_x, end_y, 2.0, color);
        draw_line(x, y, start_x, start_y, 2.0, color);
        draw_line(x, y, end_x, end_y, 2.0, color);
    }
}

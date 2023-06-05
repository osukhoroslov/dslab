use druid::Color;

use crate::app_data::AppData;

#[derive(Debug, PartialEq, Clone)]
pub struct TaskInfo {
    pub id: usize,
    pub scheduled: f64,
    pub started: f64,
    pub completed: f64,
    pub cores: u32,
    pub name: String,
}

impl TaskInfo {
    // some random colors
    const COLORS: [Color; 10] = [
        Color::rgb8(63, 167, 214),
        Color::rgb8(250, 192, 94),
        Color::rgb8(89, 205, 144),
        Color::rgb8(247, 157, 132),
        Color::rgb8(97, 155, 138),
        Color::rgb8(253, 245, 191),
        Color::rgb8(139, 184, 168),
        Color::rgb8(161, 22, 146),
        Color::rgb8(74, 88, 153),
        Color::rgb8(221, 190, 168),
    ];

    pub fn get_color(&self, data: &AppData) -> Color {
        Self::COLORS[self.get_color_hash(data) % Self::COLORS.len()].clone()
    }

    fn get_color_hash(&self, data: &AppData) -> usize {
        if data.color_by_prefix {
            Self::string_hash(self.name.split_once('_').map(|s| s.0).unwrap_or(&self.name))
        } else {
            self.id
        }
    }

    fn string_hash(s: &str) -> usize {
        const BASE: usize = 997;
        let mut res: usize = 0;
        for &b in s.as_bytes() {
            res = res.wrapping_mul(BASE).wrapping_add(b as usize)
        }
        res
    }
}

#[derive(Debug)]
pub struct File {
    pub start: f64,
    pub uploaded: f64,
    pub end: f64,
    pub name: String,
}

#[derive(Debug)]
pub struct Transfer {
    pub start: f64,
    pub end: f64,
    pub from: String,
    pub to: String,
    pub name: String,
    pub data_item_id: usize,
}

#[derive(Debug)]
pub struct Compute {
    pub name: String,
    pub speed: f64,
    pub cores: u32,
    pub memory: u64,
    pub files: Vec<File>,
    pub tasks: Vec<usize>,
}

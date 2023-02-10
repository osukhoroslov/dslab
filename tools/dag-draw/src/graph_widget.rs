use druid::kurbo::{Circle, CircleSegment, Line};
use druid::widget::prelude::*;
use druid::widget::Widget;
use druid::{Color, LifeCycle, MouseButton, Point, Size};
use std::collections::{BTreeSet, HashMap};
use std::f64::consts::PI;

use crate::app_data::*;
use crate::draw_utils::*;

const NODE_RADIUS: f64 = 30.;

const BACKGROUND: Color = Color::rgb8(0x29, 0x29, 0x29);

pub struct GraphWidget {
    nodes: Vec<Point>,
    edges: Vec<(usize, usize)>,
    last_mouse_position: Option<Point>,
    has_input: bool,
    has_output: bool,
}

impl GraphWidget {
    pub fn new() -> Self {
        GraphWidget {
            nodes: Vec::new(),
            edges: Vec::new(),
            last_mouse_position: None,
            has_input: false,
            has_output: false,
        }
    }

    fn init(&mut self, size: Size, data: &AppData) {
        let graph = data.graph.borrow();
        // last 2 nodes are for input and output
        self.nodes.resize(graph.tasks.len() + 2, Point::new(0., 0.));

        self.edges.clear();
        let mut used_data_items: BTreeSet<usize> = BTreeSet::new();
        for (i, task) in graph.tasks.iter().enumerate() {
            for &output in task.outputs.iter() {
                used_data_items.insert(output);
                for &consumer in graph.data_items[output].consumers.iter() {
                    self.edges.push((i, consumer));
                }
                if graph.data_items[output].consumers.is_empty() {
                    self.edges.push((i, self.nodes.len() - 1));
                }
            }
        }
        for data_item in 0..graph.data_items.len() {
            if used_data_items.contains(&data_item) {
                continue;
            }

            for &consumer in graph.data_items[data_item].consumers.iter() {
                self.edges.push((self.nodes.len() - 2, consumer));
            }
        }

        self.has_input = false;
        self.has_output = false;
        for &(from, to) in self.edges.iter() {
            if from == self.nodes.len() - 2 {
                self.has_input = true;
            }
            if to == self.nodes.len() - 1 {
                self.has_output = true;
            }
        }

        self.init_nodes(size);
    }

    fn init_nodes(&mut self, size: Size) {
        let mut g: Vec<Vec<usize>> = vec![Vec::new(); self.nodes.len()];

        for &(u, v) in self.edges.iter() {
            g[u].push(v);
        }

        let mut level: Vec<i32> = vec![0; self.nodes.len()];
        let mut used: Vec<bool> = vec![false; self.nodes.len()];
        let mut by_level: HashMap<i32, Vec<usize>> = HashMap::new();

        for v in 0..self.nodes.len() {
            if v == self.nodes.len() - 2 && !self.has_input {
                continue;
            }
            if v == self.nodes.len() - 1 && !self.has_output {
                continue;
            }
            if !used[v] {
                Self::dfs(v, &g, &mut level, &mut used, &mut by_level)
            }
        }

        let min_level = level.iter().min().unwrap();
        let max_level = level.iter().max().unwrap();

        let left_x = NODE_RADIUS * 2.;
        let right_x = size.width - NODE_RADIUS * 2.;

        for (level, tasks) in by_level.iter() {
            let x =
                ((level - min_level) as f64 + 0.5) / (max_level - min_level + 1) as f64 * (left_x - right_x) + right_x;
            let top_y = NODE_RADIUS * 2.;
            let bottom_y = size.height - NODE_RADIUS * 2.;
            for (ind, &task_id) in tasks.iter().enumerate() {
                let y = (ind as f64 + 0.5) / tasks.len() as f64 * (bottom_y - top_y) + top_y;
                self.nodes[task_id] = Point::new(x, y);
            }
        }
    }

    fn dfs(
        v: usize,
        g: &Vec<Vec<usize>>,
        level: &mut Vec<i32>,
        used: &mut Vec<bool>,
        by_level: &mut HashMap<i32, Vec<usize>>,
    ) {
        used[v] = true;
        for &k in g[v].iter() {
            if !used[k] {
                Self::dfs(k, g, level, used, by_level);
            }
            level[v] = level[v].max(level[k] + 1);
        }
        by_level.entry(level[v]).or_default().push(v);
    }
}

impl Widget<AppData> for GraphWidget {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event, data: &mut AppData, _: &Env) {
        match event {
            Event::MouseDown(e) => {
                // select task
                data.selected_task = None;
                for task_id in (0..self.nodes.len()).rev() {
                    if self.nodes[task_id].distance(e.pos) < NODE_RADIUS {
                        data.selected_task = Some(task_id);
                        break;
                    }
                }
                if let Some(task_id) = data.selected_task {
                    data.selected_task_info = get_text_task_info(data, task_id);
                    self.last_mouse_position = Some(e.pos);
                } else {
                    data.selected_task_info = String::new();
                }

                ctx.request_paint();
            }
            Event::MouseUp(_) => {
                self.last_mouse_position = None;
            }
            Event::MouseMove(e) => {
                if e.buttons.contains(MouseButton::Left) && self.last_mouse_position.is_some() {
                    self.nodes[data.selected_task.unwrap()] += e.pos - self.last_mouse_position.unwrap();
                    self.last_mouse_position = Some(e.pos);
                    ctx.request_paint();
                }
            }
            _ => {}
        }
    }

    fn lifecycle(&mut self, ctx: &mut LifeCycleCtx, lc: &LifeCycle, data: &AppData, _: &Env) {
        match lc {
            LifeCycle::WidgetAdded => {
                self.init(ctx.size(), data);
            }
            LifeCycle::Size(size) => {
                self.init_nodes(*size);
            }
            _ => {}
        };
    }
    fn update(&mut self, ctx: &mut UpdateCtx, _: &AppData, _: &AppData, _: &Env) {
        ctx.request_paint();
    }
    fn layout(&mut self, _: &mut LayoutCtx, bc: &BoxConstraints, _: &AppData, _: &Env) -> druid::Size {
        bc.max()
    }

    fn paint(&mut self, ctx: &mut PaintCtx, data: &AppData, _: &Env) {
        for &(from, to) in self.edges.iter() {
            ctx.stroke(Line::new(self.nodes[from], self.nodes[to]), &Color::WHITE, 1.);
        }

        let time = data.slider * data.total_time;

        for task_id in 0..self.nodes.len() - 2 {
            ctx.fill(Circle::new(self.nodes[task_id], NODE_RADIUS), &BACKGROUND);
            if let Some(task_info) = &data.task_info.borrow()[task_id] {
                if task_info.scheduled < time {
                    if time < task_info.started {
                        ctx.fill(
                            CircleSegment::new(
                                self.nodes[task_id],
                                NODE_RADIUS * 0.7,
                                NODE_RADIUS * 0.6,
                                -PI / 2.,
                                (time - task_info.scheduled) / (task_info.started - task_info.scheduled) * PI * 2.,
                            ),
                            &task_info.color,
                        );
                    } else {
                        ctx.fill(
                            CircleSegment::new(
                                self.nodes[task_id],
                                NODE_RADIUS,
                                NODE_RADIUS * 0.6,
                                -PI / 2.,
                                ((time - task_info.started) / (task_info.completed - task_info.started)).min(1.)
                                    * PI
                                    * 2.,
                            ),
                            &task_info.color,
                        );
                    }
                }
            }
            ctx.stroke(
                Circle::new(self.nodes[task_id], NODE_RADIUS),
                &Color::WHITE,
                if data.selected_task.is_some() && data.selected_task.unwrap() == task_id {
                    5.
                } else {
                    1.
                },
            );
            paint_text(ctx, &task_id.to_string(), 20., self.nodes[task_id], true, true);
        }

        // input
        if self.has_input {
            ctx.fill(Circle::new(self.nodes[self.nodes.len() - 2], NODE_RADIUS), &BACKGROUND);
            ctx.stroke(
                Circle::new(self.nodes[self.nodes.len() - 2], NODE_RADIUS),
                &Color::WHITE,
                1.,
            );
            paint_text(ctx, "input", 18., self.nodes[self.nodes.len() - 2], true, true);
        }

        // output
        if self.has_output {
            ctx.fill(Circle::new(self.nodes[self.nodes.len() - 1], NODE_RADIUS), &BACKGROUND);
            ctx.stroke(
                Circle::new(self.nodes[self.nodes.len() - 1], NODE_RADIUS),
                &Color::WHITE,
                1.,
            );
            paint_text(ctx, "output", 18., self.nodes[self.nodes.len() - 1], true, true);
        }
    }
}

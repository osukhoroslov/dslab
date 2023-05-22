use druid::kurbo::{Circle, CircleSegment, Line};
use druid::widget::prelude::*;
use druid::widget::Widget;
use druid::{Color, LifeCycle, MouseButton, Point, Size};
use std::collections::{BTreeSet, HashMap};
use std::f64::consts::PI;

use crate::app_data::*;
use crate::draw_utils::*;

const MIN_NODE_RADIUS: f64 = 30.;
const MAX_NODE_RADIUS: f64 = 60.;
const MIN_EDGE_WIDTH: f64 = 1.;
const MAX_EDGE_WIDTH: f64 = 10.;

const BACKGROUND: Color = Color::rgb8(0x29, 0x29, 0x29);

pub struct GraphWidget {
    nodes: Vec<Point>,
    names: Vec<String>,
    edges: Vec<(usize, usize, f64)>,
    radius: Vec<f64>,
    last_mouse_position: Option<Point>,
    has_input: bool,
    has_output: bool,
}

impl GraphWidget {
    pub fn new() -> Self {
        GraphWidget {
            nodes: Vec::new(),
            edges: Vec::new(),
            names: Vec::new(),
            radius: Vec::new(),
            last_mouse_position: None,
            has_input: false,
            has_output: false,
        }
    }

    fn init(&mut self, size: Size, data: &AppData) {
        let graph = data.graph.borrow();
        // last 2 nodes are for input and output
        self.nodes.resize(graph.tasks.len() + 2, Point::new(0., 0.));
        self.names = graph.tasks.iter().map(|t| t.name.clone()).collect();

        self.edges.clear();
        let mut used_data_items: BTreeSet<usize> = BTreeSet::new();
        for (i, task) in graph.tasks.iter().enumerate() {
            for &output in task.outputs.iter() {
                used_data_items.insert(output);
                for &consumer in graph.data_items[output].consumers.iter() {
                    self.edges.push((i, consumer, graph.data_items[output].size));
                }
                if graph.data_items[output].consumers.is_empty() {
                    self.edges
                        .push((i, self.nodes.len() - 1, graph.data_items[output].size));
                }
            }
        }
        for data_item in 0..graph.data_items.len() {
            if used_data_items.contains(&data_item) {
                continue;
            }

            for &consumer in graph.data_items[data_item].consumers.iter() {
                self.edges
                    .push((self.nodes.len() - 2, consumer, graph.data_items[data_item].size));
            }
        }

        self.has_input = false;
        self.has_output = false;
        for &(from, to, _w) in self.edges.iter() {
            if from == self.nodes.len() - 2 {
                self.has_input = true;
            }
            if to == self.nodes.len() - 1 {
                self.has_output = true;
            }
        }

        self.init_nodes(size, data);
    }

    fn init_nodes(&mut self, size: Size, data: &AppData) {
        let graph = data.graph.borrow();

        self.radius.clear();
        self.radius.resize(graph.tasks.len() + 2, MIN_NODE_RADIUS);

        if data.graph_variable_node_size {
            let min_task_size = (0..graph.tasks.len())
                .map(|task| graph.tasks[task].flops)
                .min_by(|a, b| a.total_cmp(b))
                .unwrap_or_default()
                .max(1.)
                .ln();
            let max_task_size = (0..graph.tasks.len())
                .map(|task| graph.tasks[task].flops)
                .max_by(|a, b| a.total_cmp(b))
                .unwrap_or_default()
                .max(1.)
                .ln();
            for task in 0..graph.tasks.len() {
                let task_size = data.graph.borrow().tasks[task].flops.max(1.).ln();
                self.radius[task] = if min_task_size == max_task_size {
                    MIN_NODE_RADIUS
                } else {
                    (task_size - min_task_size) / (max_task_size - min_task_size) * (MAX_NODE_RADIUS - MIN_NODE_RADIUS)
                        + MIN_NODE_RADIUS
                };
            }
        }

        let mut g: Vec<Vec<usize>> = vec![Vec::new(); self.nodes.len()];

        for &(u, v, _w) in self.edges.iter() {
            if data.graph_levels_from_end {
                g[u].push(v);
            } else {
                g[v].push(u);
            }
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

        let mut left_x = size.width - MIN_NODE_RADIUS * 2.;
        let mut right_x = MIN_NODE_RADIUS * 2.;

        if data.graph_levels_from_end {
            std::mem::swap(&mut left_x, &mut right_x);
        }

        for (level, tasks) in by_level.iter() {
            let x =
                ((level - min_level) as f64 + 0.5) / (max_level - min_level + 1) as f64 * (left_x - right_x) + right_x;
            let top_y = MIN_NODE_RADIUS * 2.;
            let bottom_y = size.height - MIN_NODE_RADIUS * 2.;
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
                    if self.nodes[task_id].distance(e.pos) < self.radius[task_id] {
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
                self.init_nodes(*size, data);
            }
            _ => {}
        };
    }
    fn update(&mut self, ctx: &mut UpdateCtx, old_data: &AppData, data: &AppData, _: &Env) {
        if old_data.graph_levels_from_end != data.graph_levels_from_end
            || old_data.graph_variable_edge_width != data.graph_variable_edge_width
            || old_data.graph_variable_node_size != data.graph_variable_node_size
        {
            self.init_nodes(ctx.size(), data);
        }
        ctx.request_paint();
    }
    fn layout(&mut self, _: &mut LayoutCtx, bc: &BoxConstraints, _: &AppData, _: &Env) -> druid::Size {
        bc.max()
    }

    fn paint(&mut self, ctx: &mut PaintCtx, data: &AppData, _: &Env) {
        let max_width = self
            .edges
            .iter()
            .map(|x| x.2)
            .max_by(|a, b| a.total_cmp(b))
            .unwrap_or_default();
        for &(from, to, mut w) in self.edges.iter() {
            if !data.graph_variable_edge_width {
                w = 0.;
            }
            ctx.stroke(
                Line::new(self.nodes[from], self.nodes[to]),
                &Color::WHITE,
                (w / max_width * MAX_EDGE_WIDTH).max(MIN_EDGE_WIDTH),
            );
        }

        let time = data.slider * data.total_time;

        for task_id in 0..self.nodes.len() - 2 {
            let radius = self.radius[task_id];
            ctx.fill(Circle::new(self.nodes[task_id], radius), &BACKGROUND);
            if let Some(task_info) = &data.task_info.borrow()[task_id] {
                if task_info.scheduled < time {
                    if time < task_info.started {
                        ctx.fill(
                            CircleSegment::new(
                                self.nodes[task_id],
                                radius * 0.7,
                                radius * 0.6,
                                -PI / 2.,
                                (time - task_info.scheduled) / (task_info.started - task_info.scheduled) * PI * 2.,
                            ),
                            &task_info.color,
                        );
                    } else {
                        ctx.fill(
                            CircleSegment::new(
                                self.nodes[task_id],
                                radius,
                                radius * 0.6,
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
                Circle::new(self.nodes[task_id], radius),
                &Color::WHITE,
                if data.selected_task.is_some() && data.selected_task.unwrap() == task_id {
                    5.
                } else {
                    1.
                },
            );
            if data.graph_show_task_names {
                paint_text(
                    ctx,
                    &self.names[task_id],
                    radius * 3.5 / self.names[task_id].len().max(6) as f64,
                    self.nodes[task_id],
                    true,
                    true,
                );
            } else {
                paint_text(ctx, &task_id.to_string(), 20., self.nodes[task_id], true, true);
            }
        }

        // input
        if self.has_input {
            ctx.fill(
                Circle::new(self.nodes[self.nodes.len() - 2], MIN_NODE_RADIUS),
                &BACKGROUND,
            );
            ctx.stroke(
                Circle::new(self.nodes[self.nodes.len() - 2], MIN_NODE_RADIUS),
                &Color::WHITE,
                1.,
            );
            paint_text(ctx, "input", 18., self.nodes[self.nodes.len() - 2], true, true);
        }

        // output
        if self.has_output {
            ctx.fill(
                Circle::new(self.nodes[self.nodes.len() - 1], MIN_NODE_RADIUS),
                &BACKGROUND,
            );
            ctx.stroke(
                Circle::new(self.nodes[self.nodes.len() - 1], MIN_NODE_RADIUS),
                &Color::WHITE,
                1.,
            );
            paint_text(ctx, "output", 18., self.nodes[self.nodes.len() - 1], true, true);
        }
    }
}

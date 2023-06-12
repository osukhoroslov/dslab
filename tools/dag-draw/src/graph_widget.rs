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

#[derive(Clone)]
struct Node {
    node_type: NodeType,
    name: String,
    pos: Point,
    radius: f64,
}

pub struct GraphWidget {
    nodes: Vec<Node>,
    edges: Vec<(usize, usize, f64)>,
    last_mouse_position: Option<Point>,
    selected_node_ind: Option<usize>,
}

impl GraphWidget {
    pub fn new() -> Self {
        GraphWidget {
            nodes: Vec::new(),
            edges: Vec::new(),
            last_mouse_position: None,
            selected_node_ind: None,
        }
    }

    fn init(&mut self, size: Size, data: &AppData) {
        let graph = data.graph.borrow();

        self.nodes.clear();
        for (task_id, task) in graph.tasks.iter().enumerate() {
            self.nodes.push(Node {
                node_type: NodeType::Task(task_id),
                name: task.name.clone(),
                pos: Point::new(0., 0.),
                radius: MIN_NODE_RADIUS,
            });
        }

        let mut inputs = (0..graph.data_items.len()).collect::<BTreeSet<usize>>();
        let mut outputs = (0..graph.data_items.len()).collect::<BTreeSet<usize>>();

        for task in graph.tasks.iter() {
            for output in task.outputs.iter() {
                inputs.remove(output);
            }
            for input in task.inputs.iter() {
                outputs.remove(input);
            }
        }

        let inputs = inputs
            .into_iter()
            .map(|input| {
                self.nodes.push(Node {
                    node_type: NodeType::Input(input),
                    name: graph.data_items[input].name.clone(),
                    pos: Point::new(0., 0.),
                    radius: MIN_NODE_RADIUS,
                });
                (input, self.nodes.len() - 1)
            })
            .collect::<HashMap<usize, usize>>();
        let outputs = outputs
            .into_iter()
            .map(|output| {
                self.nodes.push(Node {
                    node_type: NodeType::Output(output),
                    name: graph.data_items[output].name.clone(),
                    pos: Point::new(0., 0.),
                    radius: MIN_NODE_RADIUS,
                });
                (output, self.nodes.len() - 1)
            })
            .collect::<HashMap<usize, usize>>();

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
        if min_task_size != max_task_size {
            for task in 0..graph.tasks.len() {
                let task_size = data.graph.borrow().tasks[task].flops.max(1.).ln();
                self.nodes[task].radius = (task_size - min_task_size) / (max_task_size - min_task_size)
                    * (MAX_NODE_RADIUS - MIN_NODE_RADIUS)
                    + MIN_NODE_RADIUS;
            }
        }

        self.edges.clear();
        for (i, task) in graph.tasks.iter().enumerate() {
            for &output in task.outputs.iter() {
                if let Some(ind) = outputs.get(&output) {
                    self.edges.push((i, *ind, graph.data_items[output].size));
                } else {
                    for &consumer in graph.data_items[output].consumers.iter() {
                        self.edges.push((i, consumer, graph.data_items[output].size));
                    }
                }
            }

            for &input in task.inputs.iter() {
                if let Some(ind) = inputs.get(&input) {
                    self.edges.push((*ind, i, graph.data_items[input].size));
                }
            }
        }

        self.init_nodes(size, data);
    }

    fn init_nodes(&mut self, size: Size, data: &AppData) {
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
            if !used[v] {
                Self::dfs(v, &g, &mut level, &mut used, &mut by_level)
            }
        }

        let mut to_add: Vec<(usize, i32)> = Vec::new();

        let min_level = *level.iter().min().unwrap();
        let max_level = *level.iter().max().unwrap();

        for (&level, tasks) in by_level.iter_mut() {
            tasks.retain(|&task| {
                let target_level = match self.nodes[task].node_type {
                    NodeType::Input(_) => {
                        if data.graph_levels_from_end {
                            max_level
                        } else {
                            min_level
                        }
                    }
                    NodeType::Output(_) => {
                        if data.graph_levels_from_end {
                            min_level
                        } else {
                            max_level
                        }
                    }
                    NodeType::Task(_) => level,
                };
                if target_level != level {
                    to_add.push((task, target_level));
                    false
                } else {
                    true
                }
            });
        }

        for (task, target_level) in to_add {
            by_level.entry(target_level).or_default().push(task);
        }

        let mut left_x = size.width - MIN_NODE_RADIUS * 2.;
        let mut right_x = MIN_NODE_RADIUS * 2.;

        if data.graph_levels_from_end {
            std::mem::swap(&mut left_x, &mut right_x);
        }

        for (level, mut tasks) in by_level.into_iter() {
            tasks.sort();
            let x =
                ((level - min_level) as f64 + 0.5) / (max_level - min_level + 1) as f64 * (left_x - right_x) + right_x;
            let top_y = MIN_NODE_RADIUS * 2.;
            let bottom_y = size.height - MIN_NODE_RADIUS * 2.;
            for (ind, &task_id) in tasks.iter().enumerate() {
                let y = (ind as f64 + 0.5) / tasks.len() as f64 * (bottom_y - top_y) + top_y;
                self.nodes[task_id].pos = Point::new(x, y);
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
                data.selected_node = None;
                self.selected_node_ind = None;
                for node_id in (0..self.nodes.len()).rev() {
                    let radius = if data.graph_variable_node_size {
                        self.nodes[node_id].radius
                    } else {
                        MIN_NODE_RADIUS
                    };
                    if self.nodes[node_id].pos.distance(e.pos) < radius {
                        data.selected_node = Some(self.nodes[node_id].node_type);
                        self.selected_node_ind = Some(node_id);
                        break;
                    }
                }
                if let Some(node_type) = data.selected_node {
                    data.selected_node_info = get_text_node_info(data, node_type);
                    self.last_mouse_position = Some(e.pos);
                } else {
                    data.selected_node_info = String::new();
                }

                ctx.request_paint();
            }
            Event::MouseUp(_) => {
                self.last_mouse_position = None;
                self.selected_node_ind = None;
            }
            Event::MouseMove(e) => {
                if e.buttons.contains(MouseButton::Left) && self.last_mouse_position.is_some() {
                    self.nodes[self.selected_node_ind.unwrap()].pos += e.pos - self.last_mouse_position.unwrap();
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
        if old_data.graph_levels_from_end != data.graph_levels_from_end {
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
                Line::new(self.nodes[from].pos, self.nodes[to].pos),
                &Color::WHITE,
                (w / max_width * MAX_EDGE_WIDTH).max(MIN_EDGE_WIDTH),
            );
        }

        let time = data.slider * data.total_time;

        for task_id in 0..self.nodes.len() {
            let radius = if data.graph_variable_node_size {
                self.nodes[task_id].radius
            } else {
                MIN_NODE_RADIUS
            };
            ctx.fill(Circle::new(self.nodes[task_id].pos, radius), &BACKGROUND);
            let stroke_width = if data.selected_node == Some(self.nodes[task_id].node_type) {
                5.
            } else {
                1.
            };
            match self.nodes[task_id].node_type {
                NodeType::Task(_) => {
                    if let Some(task_info) = &data.task_info.borrow()[task_id] {
                        if task_info.scheduled < time {
                            if time < task_info.started {
                                ctx.fill(
                                    CircleSegment::new(
                                        self.nodes[task_id].pos,
                                        radius * 0.7,
                                        radius * 0.6,
                                        -PI / 2.,
                                        (time - task_info.scheduled) / (task_info.started - task_info.scheduled)
                                            * PI
                                            * 2.,
                                    ),
                                    &task_info.get_color(data),
                                );
                            } else {
                                ctx.fill(
                                    CircleSegment::new(
                                        self.nodes[task_id].pos,
                                        radius,
                                        radius * 0.6,
                                        -PI / 2.,
                                        ((time - task_info.started) / (task_info.completed - task_info.started))
                                            .min(1.)
                                            * PI
                                            * 2.,
                                    ),
                                    &task_info.get_color(data),
                                );
                            }
                        }
                    }
                    ctx.stroke(
                        Circle::new(self.nodes[task_id].pos, radius),
                        &Color::WHITE,
                        stroke_width,
                    );
                    if data.graph_show_task_names {
                        paint_text(
                            ctx,
                            &self.nodes[task_id].name,
                            radius * 3.5 / self.nodes[task_id].name.len().max(6) as f64,
                            self.nodes[task_id].pos,
                            true,
                            true,
                        );
                    } else {
                        paint_text(ctx, &task_id.to_string(), 20., self.nodes[task_id].pos, true, true);
                    }
                }
                NodeType::Input(_) => {
                    ctx.fill(Circle::new(self.nodes[task_id].pos, radius), &BACKGROUND);
                    ctx.stroke(
                        Circle::new(self.nodes[task_id].pos, radius),
                        &Color::WHITE,
                        stroke_width,
                    );
                    if data.graph_show_task_names {
                        paint_text(
                            ctx,
                            &self.nodes[task_id].name,
                            radius * 3.5 / self.nodes[task_id].name.len().max(6) as f64,
                            self.nodes[task_id].pos,
                            true,
                            true,
                        );
                    } else {
                        paint_text(ctx, "input", 18., self.nodes[task_id].pos, true, true);
                    }
                }
                NodeType::Output(_) => {
                    ctx.fill(Circle::new(self.nodes[task_id].pos, radius), &BACKGROUND);
                    ctx.stroke(
                        Circle::new(self.nodes[task_id].pos, radius),
                        &Color::WHITE,
                        stroke_width,
                    );
                    if data.graph_show_task_names {
                        paint_text(
                            ctx,
                            &self.nodes[task_id].name,
                            radius * 3.5 / self.nodes[task_id].name.len().max(6) as f64,
                            self.nodes[task_id].pos,
                            true,
                            true,
                        );
                    } else {
                        paint_text(ctx, "output", 18., self.nodes[task_id].pos, true, true);
                    }
                }
            }
        }
    }
}

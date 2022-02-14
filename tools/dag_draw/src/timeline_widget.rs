use druid::kurbo::Line;
use druid::widget::prelude::*;
use druid::widget::Widget;
use druid::{Color, Rect};
use druid::{Point, Size};

use crate::app_data::*;
use crate::draw_utils::*;

const X_PADDING: f64 = 30.0;
const ROW_STEP: f64 = 20.0;
const MEMORY_HEIGHT: f64 = 100.0;

struct TimelineResourceBlock {
    start: f64,
    end: f64,
    height: f64, // in [0, 1]
    color: Color,
    selected: bool,
    task: usize,
}

impl TimelineResourceBlock {
    fn new(start: f64, end: f64, height: f64, color: Color, selected: bool, task: usize) -> Self {
        TimelineResourceBlock {
            start,
            end,
            height,
            color,
            selected,
            task,
        }
    }
}

pub struct TimelineWidget {
    timeline_left: f64,
    timeline_right: f64,
    total_time: f64,
    size: Size,
    clickable_rectangles: Vec<(Rect, usize)>,
}

impl TimelineWidget {
    pub fn new() -> Self {
        TimelineWidget {
            timeline_left: 0.,
            timeline_right: 0.,
            total_time: 0.,
            size: Size::new(0., 0.),
            clickable_rectangles: Vec::new(),
        }
    }

    fn get_time_x(&self, time: f64) -> f64 {
        time / self.total_time * (self.timeline_right - self.timeline_left) + self.timeline_left
    }

    fn draw_resource_usage(&mut self, ctx: &mut PaintCtx, y: f64, height: f64, usages: Vec<TimelineResourceBlock>) {
        let mut events: Vec<f64> = Vec::new();
        for item in usages.iter() {
            events.push(item.start);
            events.push(item.end);
        }
        events.sort_by(|a, b| a.partial_cmp(b).unwrap());
        events.dedup();
        for i in 0..events.len() - 1 {
            let left = events[i];
            let right = events[i + 1];
            let mut cury = y;
            for usage in usages.iter() {
                if usage.end <= left || usage.start >= right {
                    continue;
                }
                let rect = Rect::from_points(
                    Point::new(self.get_time_x(left), cury),
                    Point::new(self.get_time_x(right), cury + usage.height * height),
                );
                ctx.fill(rect.clone(), &usage.color);
                self.clickable_rectangles.push((rect, usage.task));
                cury += usage.height * height;
            }
        }
        for i in 0..events.len() - 1 {
            let left = events[i];
            let right = events[i + 1];
            let mut cury = y;
            for usage in usages.iter() {
                if usage.end <= left || usage.start >= right {
                    continue;
                }
                if usage.selected {
                    ctx.stroke(
                        Rect::from_points(
                            Point::new(self.get_time_x(left), cury),
                            Point::new(self.get_time_x(right), cury + usage.height * height),
                        ),
                        &Color::WHITE,
                        5.,
                    );
                }
                cury += usage.height * height;
            }
        }
    }
}

impl Widget<AppData> for TimelineWidget {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event, data: &mut AppData, _: &Env) {
        match event {
            Event::MouseDown(e) => {
                data.selected_task = None;
                for (rect, task) in self.clickable_rectangles.iter() {
                    if rect.contains(e.pos) {
                        data.selected_task = Some(task.clone());
                        break;
                    }
                }
                if let Some(task_id) = data.selected_task {
                    data.selected_task_info = get_text_task_info(data, task_id);
                } else {
                    data.selected_task_info = String::new();
                }
                ctx.request_paint();
            }
            _ => {}
        }
    }

    fn lifecycle(&mut self, _: &mut LifeCycleCtx, _: &LifeCycle, _: &AppData, _: &Env) {}
    fn update(&mut self, ctx: &mut UpdateCtx, _: &AppData, _: &AppData, _: &Env) {
        ctx.request_paint();
    }
    fn layout(&mut self, _: &mut LayoutCtx, bc: &BoxConstraints, _: &AppData, _: &Env) -> druid::Size {
        bc.max()
    }

    fn paint(&mut self, ctx: &mut PaintCtx, data: &AppData, _: &Env) {
        let size = ctx.size();
        self.clickable_rectangles.clear();

        let timeline_left = X_PADDING + 150.;
        let timeline_right = size.width - X_PADDING;

        self.size = size;
        self.timeline_left = timeline_left;
        self.timeline_right = timeline_right;
        self.total_time = data.total_time;

        let mut y = 20.;
        for compute in data.compute.borrow().iter() {
            let y0 = y;
            paint_text(ctx, &compute.name, 25., Point::new(X_PADDING + 5., y), false, false);

            y += 35.;

            // download
            if data.timeline_downloading {
                ctx.stroke(
                    Line::new(Point::new(X_PADDING, y), Point::new(size.width - X_PADDING, y)),
                    &Color::WHITE,
                    2.,
                );
                for transfer in data.transfers.borrow().iter() {
                    if transfer.to == compute.name {
                        paint_text(ctx, &transfer.name, 15., Point::new(X_PADDING + 15., y), false, false);

                        ctx.stroke(
                            Rect::from_points(
                                Point::new(X_PADDING, y),
                                Point::new(size.width - X_PADDING, y + ROW_STEP),
                            ),
                            &Color::WHITE,
                            0.1,
                        );
                        draw_download(ctx, Point::new(X_PADDING + 8., y + 10.));

                        ctx.fill(
                            Rect::from_points(
                                Point::new(self.get_time_x(transfer.start), y + 5.),
                                Point::new(self.get_time_x(transfer.end), y + ROW_STEP - 5.),
                            ),
                            if data.selected_task.is_some() && data.selected_task.unwrap() == transfer.task {
                                &Color::WHITE
                            } else {
                                &Color::GRAY
                            },
                        );

                        y += ROW_STEP;
                    }
                }
            }

            // cores
            if data.timeline_cores {
                let mut cores = Vec::new();
                for &task_id in compute.tasks.iter() {
                    let task_info = data.task_info.borrow()[task_id].as_ref().unwrap().clone();
                    cores.push(TimelineResourceBlock::new(
                        task_info.scheduled,
                        task_info.completed,
                        task_info.cores as f64 / compute.cores as f64,
                        task_info.color.clone(),
                        data.selected_task.is_some() && data.selected_task.unwrap() == task_id,
                        task_id,
                    ));
                }
                self.draw_resource_usage(ctx, y, compute.cores as f64 * ROW_STEP, cores);
                ctx.stroke(
                    Line::new(Point::new(X_PADDING, y), Point::new(size.width - X_PADDING, y)),
                    &Color::WHITE,
                    2.,
                );
                paint_text(
                    ctx,
                    &format!("Cores: {}", compute.cores),
                    15.,
                    Point::new(X_PADDING + 5., y + compute.cores as f64 * ROW_STEP / 2. - 10.),
                    false,
                    false,
                );
                for _i in 0..compute.cores {
                    ctx.stroke(
                        Rect::from_points(Point::new(timeline_left, y), Point::new(timeline_right, y)),
                        &Color::WHITE,
                        0.2,
                    );
                    y += ROW_STEP;
                }
            }

            // memory
            if data.timeline_memory {
                let mut memory = Vec::new();
                for &task_id in compute.tasks.iter() {
                    let task_info = data.task_info.borrow()[task_id].as_ref().unwrap().clone();
                    memory.push(TimelineResourceBlock::new(
                        task_info.scheduled,
                        task_info.completed,
                        data.graph.borrow().tasks[task_id].memory as f64 / compute.memory as f64,
                        task_info.color.clone(),
                        data.selected_task.is_some() && data.selected_task.unwrap() == task_id,
                        task_id,
                    ));
                }
                self.draw_resource_usage(ctx, y, MEMORY_HEIGHT, memory);
                ctx.stroke(
                    Line::new(Point::new(X_PADDING, y), Point::new(size.width - X_PADDING, y)),
                    &Color::WHITE,
                    2.,
                );
                paint_text(
                    ctx,
                    &format!("Memory: {}", compute.memory),
                    15.,
                    Point::new(X_PADDING + 5., y + MEMORY_HEIGHT / 2. - 10.),
                    false,
                    false,
                );
                y += MEMORY_HEIGHT;
            }

            if data.timeline_uploading {
                ctx.stroke(
                    Line::new(Point::new(X_PADDING, y), Point::new(size.width - X_PADDING, y)),
                    &Color::WHITE,
                    2.,
                );

                // upload
                for transfer in data.transfers.borrow().iter() {
                    if transfer.from == compute.name {
                        paint_text(ctx, &transfer.name, 15., Point::new(X_PADDING + 15., y), false, false);

                        ctx.stroke(
                            Rect::from_points(
                                Point::new(X_PADDING, y),
                                Point::new(size.width - X_PADDING, y + ROW_STEP),
                            ),
                            &Color::WHITE,
                            0.1,
                        );
                        draw_upload(ctx, Point::new(X_PADDING + 8., y + 10.));

                        ctx.fill(
                            Rect::from_points(
                                Point::new(self.get_time_x(transfer.start), y + 5.),
                                Point::new(self.get_time_x(transfer.end), y + ROW_STEP - 5.),
                            ),
                            if data.selected_task.is_some() && data.selected_task.unwrap() == transfer.task {
                                &Color::WHITE
                            } else {
                                &Color::GRAY
                            },
                        );

                        y += ROW_STEP;
                    }
                }
            }
            ctx.stroke(
                Line::new(Point::new(X_PADDING, y), Point::new(size.width - X_PADDING, y)),
                &Color::WHITE,
                1.,
            );

            ctx.stroke(
                Rect::from_points(Point::new(X_PADDING, y0), Point::new(size.width - X_PADDING, y)),
                &Color::WHITE,
                3.,
            );
            ctx.stroke(
                Line::new(Point::new(timeline_left, y0 + 35.), Point::new(timeline_left, y)),
                &Color::WHITE,
                2.,
            );

            y += 50.;
        }

        let time = self.get_time_x(data.slider * data.total_time);
        ctx.stroke(
            Line::new(Point::new(time, 0.), Point::new(time, y - 30.)),
            &Color::RED,
            2.,
        );
    }
}

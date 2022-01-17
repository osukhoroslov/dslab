use druid::kurbo::Line;
use druid::piet::{FontFamily, Text, TextLayout, TextLayoutBuilder};
use druid::widget::prelude::*;
use druid::widget::Widget;
use druid::{Color, Rect};
use druid::{Point, Size};

use crate::app_data::*;
use crate::data::*;

const BLOCK_WIDTH: f64 = 300.0;
const LABEL_WIDTH: f64 = BLOCK_WIDTH / 3.0;
const CORE_SIZE: f64 = 15.0;

pub struct DrawingWidget {}

impl DrawingWidget {
    fn paint_colored_text(
        &mut self,
        ctx: &mut PaintCtx,
        text: &str,
        font: f64,
        mut pos: Point,
        centerx: bool,
        color: Color,
    ) {
        let layout = ctx
            .text()
            .new_text_layout(text.to_string())
            .font(FontFamily::SYSTEM_UI, font)
            .text_color(color)
            .build()
            .unwrap();
        let text_size = layout.size();
        if centerx {
            pos.x -= text_size.width / 2.;
        }
        ctx.draw_text(&layout, pos);
    }

    fn paint_text(&mut self, ctx: &mut PaintCtx, text: &str, font: f64, pos: Point, centerx: bool) {
        self.paint_colored_text(ctx, text, font, pos, centerx, Color::WHITE);
    }

    fn draw_arrow(&mut self, ctx: &mut PaintCtx, pos: Point, is_download: bool) {
        let w1 = 4.;
        let w2 = 7.;
        let mut h1 = -7.;
        let h0 = 0.;
        let mut h2 = 7.;
        if !is_download {
            std::mem::swap(&mut h1, &mut h2);
        }
        let mut arrow = vec![
            Point::new(w1, h0),
            Point::new(w1, h1),
            Point::new(-w1, h1),
            Point::new(-w1, h0),
            Point::new(-w2, h0),
            Point::new(0., h2),
            Point::new(w2, h0),
        ];
        for point in arrow.iter_mut() {
            point.x += pos.x;
            point.y += pos.y;
        }
        let poly = crate::poly::Poly::from_vec(arrow);
        ctx.fill(
            poly,
            &(if is_download {
                Color::rgb8(0, 200, 0)
            } else {
                Color::rgb8(0, 210, 210)
            }),
        );
    }

    fn draw_download(&mut self, ctx: &mut PaintCtx, pos: Point) {
        self.draw_arrow(ctx, pos, true);
    }

    fn draw_upload(&mut self, ctx: &mut PaintCtx, pos: Point) {
        self.draw_arrow(ctx, pos, false);
    }

    fn draw_actor_files_block(
        &mut self,
        ctx: &mut PaintCtx,
        data: &AppData,
        actor: &str,
        middlex: f64,
        downy: &mut f64,
        files: &Vec<File>,
    ) {
        let time = data.slider * data.total_time;

        let files_limit = match data.files_limit_str.parse::<usize>() {
            Ok(x) => x,
            Err(_) => return,
        };

        let leftx = middlex - BLOCK_WIDTH / 2.;

        let step = 20.0;

        let is_uploading = |name: &str, actor: &str| -> bool {
            for transfer in data.transfers.borrow().iter() {
                if transfer.name != name {
                    continue;
                }
                if transfer.start <= time && time < transfer.end && transfer.from == actor {
                    return true;
                }
            }
            false
        };

        // files title and border around it
        self.paint_text(ctx, "Files", 18., Point::new(leftx + 5., *downy), false);
        ctx.stroke(
            Rect::from_points(Point::new(leftx, *downy), Point::new(leftx + LABEL_WIDTH, *downy + 25.)),
            &Color::WHITE,
            1.,
        );

        *downy += 25.;

        let mut active_files: Vec<&File> = Vec::new();

        for file in files.iter() {
            if time < file.start || file.end < time {
                continue;
            }

            active_files.push(file)
        }

        let extra_files = active_files.len().max(files_limit) - files_limit;
        let extra_files_space = files_limit - active_files.len().min(files_limit);
        if extra_files > 0 {
            self.paint_text(
                ctx,
                &format!("+{}", extra_files),
                18.,
                Point::new(leftx + LABEL_WIDTH + 5., *downy - 25.),
                false,
            );
        }

        for file in active_files.into_iter().rev().take(files_limit).rev() {
            // file is downloading, uploading or neither
            if time < file.uploaded {
                self.draw_download(ctx, Point::new(middlex + 10., *downy + step / 2. + 2.5));
            } else if is_uploading(&file.name, actor) {
                self.draw_upload(ctx, Point::new(middlex + 10., *downy + step / 2. + 2.5));
            }

            self.paint_text(ctx, &file.name, 15., Point::new(leftx + 5., *downy), false);
            *downy += step;
        }

        *downy += step * extra_files_space as f64;

        *downy += 5.;
    }

    fn draw_actor_tasks_block(
        &mut self,
        ctx: &mut PaintCtx,
        data: &AppData,
        middlex: f64,
        downy: &mut f64,
        tasks: &Vec<Task>,
    ) {
        let time = data.slider * data.total_time;

        let tasks_limit = match data.tasks_limit_str.parse::<usize>() {
            Ok(x) => x,
            Err(_) => return,
        };

        let leftx = middlex - BLOCK_WIDTH / 2.;
        let rightx = middlex + BLOCK_WIDTH / 2.;

        let step = 20.0;

        // title for tasks and border around it
        self.paint_text(ctx, "Tasks", 18., Point::new(leftx + 5., *downy), false);
        ctx.stroke(
            Rect::from_points(Point::new(leftx, *downy), Point::new(leftx + LABEL_WIDTH, *downy + 25.)),
            &Color::WHITE,
            1.,
        );

        *downy += 25.;

        let mut active_tasks: Vec<&Task> = Vec::new();

        for task in tasks.iter() {
            if time < task.scheduled || task.completed < time {
                continue;
            }

            active_tasks.push(task);
        }

        let extra_tasks_space = tasks_limit - active_tasks.len().min(tasks_limit);

        for task in active_tasks.into_iter().rev().take(tasks_limit).rev() {
            // task name
            self.paint_text(ctx, &task.name, 15., Point::new(leftx + 5., *downy), false);

            // task status, either pending or progress bar
            if time < task.started {
                self.paint_colored_text(
                    ctx,
                    "pending...",
                    15.,
                    Point::new(middlex + 5., *downy),
                    false,
                    task.color.clone(),
                );
            } else {
                let mut width = BLOCK_WIDTH / 2. - 10.;
                width *= (time - task.started) / (task.completed - task.started);
                ctx.fill(
                    Rect::from_points(
                        Point::new(middlex + 5., *downy + step / 2. + 2.5 + CORE_SIZE / 2.),
                        Point::new(middlex + 5. + width, *downy + step / 2. + 2.5 - CORE_SIZE / 2.),
                    ),
                    &task.color,
                );
                ctx.stroke(
                    Rect::from_points(
                        Point::new(middlex + 5., *downy + step / 2. + 2.5 + CORE_SIZE / 2.),
                        Point::new(rightx - 5., *downy + step / 2. + 2.5 - CORE_SIZE / 2.),
                    ),
                    &Color::WHITE,
                    1.,
                );
            }

            *downy += step;
        }

        *downy += step * extra_tasks_space as f64;
        *downy += 5.;

        // line below tasks
        ctx.stroke(
            Line::new(Point::new(leftx, *downy), Point::new(rightx, *downy)),
            &Color::WHITE,
            1.,
        );
    }

    fn draw_actor(
        &mut self,
        ctx: &mut PaintCtx,
        data: &AppData,
        position: Point,
        name: &str,
        compute: Option<&Compute>,
        files: Option<&Vec<File>>,
    ) {
        let middlex = position.x;
        let leftx = middlex - BLOCK_WIDTH / 2.;
        let rightx = middlex + BLOCK_WIDTH / 2.;
        let upy = position.y;

        let time = data.slider * data.total_time;

        self.paint_text(ctx, name, 25., Point::new(middlex, upy), true);

        // line below title
        ctx.stroke(
            Line::new(Point::new(leftx, upy + 35.), Point::new(rightx, upy + 35.)),
            &Color::WHITE,
            1.,
        );

        // current bottom border
        let mut downy = upy + 35.;

        // block with cores and tasks
        if let Some(compute) = compute {
            // two titles and two borders for them
            self.paint_text(
                ctx,
                &format!("Cores: {}", compute.cores),
                18.,
                Point::new(leftx + 5., downy),
                false,
            );
            self.paint_text(
                ctx,
                &format!("Speed: {}", compute.speed),
                18.,
                Point::new(leftx + 5. + LABEL_WIDTH, downy),
                false,
            );
            ctx.stroke(
                Rect::from_points(Point::new(leftx, downy), Point::new(leftx + LABEL_WIDTH, downy + 25.)),
                &Color::WHITE,
                1.,
            );
            ctx.stroke(
                Rect::from_points(
                    Point::new(leftx + LABEL_WIDTH, downy),
                    Point::new(leftx + LABEL_WIDTH * 2., downy + 25.),
                ),
                &Color::WHITE,
                1.,
            );

            downy += 25.;

            let mut xcore = leftx + 5. + CORE_SIZE / 2.;
            let ycore = downy + 25. / 2.;

            for task in compute.tasks.iter() {
                if time < task.scheduled || task.completed < time {
                    continue;
                }

                // paint core
                ctx.fill(
                    Rect::from_center_size(Point::new(xcore, ycore), Size::new(CORE_SIZE, CORE_SIZE)),
                    &task.color,
                );
                xcore += 20.;
            }

            let xcore = leftx + 5. + CORE_SIZE / 2.;

            // cores (without filling them right now)
            for i in 0..compute.cores {
                ctx.stroke(
                    Rect::from_center_size(Point::new(xcore + (i as f64) * 20., ycore), Size::new(15., 15.)),
                    &Color::WHITE,
                    1.,
                );
            }

            downy += 25.;

            // line below cores
            ctx.stroke(
                Line::new(Point::new(leftx, downy), Point::new(rightx, downy)),
                &Color::WHITE,
                1.,
            );

            self.draw_actor_tasks_block(ctx, data, middlex, &mut downy, &compute.tasks);
        }

        // block with files
        if let Some(files) = files {
            self.draw_actor_files_block(ctx, data, name, middlex, &mut downy, &files);
        }

        // line below files
        ctx.stroke(
            Rect::from_points(Point::new(leftx, upy), Point::new(rightx, downy)),
            &Color::WHITE,
            1.,
        );
    }
}

impl Widget<AppData> for DrawingWidget {
    fn event(&mut self, ctx: &mut EventCtx, _: &Event, _: &mut AppData, _: &Env) {
        ctx.request_focus();
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
        let time = data.total_time * data.slider;

        self.draw_actor(
            ctx,
            data,
            Point::new(size.width / 2., 10.),
            "scheduler",
            None,
            Some(&data.scheduler_files.borrow()),
        );

        self.paint_text(ctx, &format!("time: {:.3}", time), 15., Point::new(0., 0.), false);

        for (i, compute) in (*data.compute.borrow()).iter().enumerate() {
            let position = Point::new(
                size.width / (data.compute.borrow().len() as f64) * (i as f64 + 0.5),
                size.height * 0.5,
            );
            self.draw_actor(ctx, data, position, &compute.name, Some(&compute), Some(&compute.files));
        }
    }
}

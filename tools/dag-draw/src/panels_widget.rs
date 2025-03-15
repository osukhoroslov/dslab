use druid::kurbo::Line;
use druid::widget::prelude::*;
use druid::widget::Widget;
use druid::{Color, Rect};
use druid::{Point, Size};

use crate::app_data::*;
use crate::data::*;
use crate::draw_utils::*;

const BLOCK_WIDTH: f64 = 305.0;
const BLOCK_X_PADDING: f64 = 10.0;
const BLOCK_Y_PADDING: f64 = 20.0;
const LABEL_WIDTH: f64 = BLOCK_WIDTH / 3.0;
const CORE_SIZE: f64 = 15.0;
const ROW_STEP: f64 = 20.0;
const CORES_PER_ROW: u32 = 15;

pub struct PanelsWidget {}

impl PanelsWidget {
    fn draw_actor_files_block(
        &mut self,
        ctx: &mut PaintCtx,
        data: &AppData,
        actor: &str,
        middlex: f64,
        downy: &mut f64,
        files: &[File],
    ) {
        let time = data.slider * data.total_time;

        let files_limit = match data.files_limit_str.parse::<usize>() {
            Ok(x) => x,
            Err(_) => return,
        };

        let leftx = middlex - BLOCK_WIDTH / 2.;

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
        paint_text(ctx, "Files", 18., Point::new(leftx + 5., *downy), false, false);
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
            paint_text(
                ctx,
                &format!("+{}", extra_files),
                18.,
                Point::new(leftx + LABEL_WIDTH + 5., *downy - 25.),
                false,
                false,
            );
        }

        for file in active_files.into_iter().rev().take(files_limit).rev() {
            // file is downloading, uploading or neither
            if time < file.uploaded {
                draw_download(ctx, Point::new(middlex + 10., *downy + ROW_STEP / 2. + 2.5));
            } else if is_uploading(&file.name, actor) {
                draw_upload(ctx, Point::new(middlex + 10., *downy + ROW_STEP / 2. + 2.5));
            }

            paint_text(ctx, &file.name, 15., Point::new(leftx + 5., *downy), false, false);
            *downy += ROW_STEP;
        }

        *downy += ROW_STEP * extra_files_space as f64;

        *downy += 5.;
    }

    fn draw_actor_tasks_block(
        &mut self,
        ctx: &mut PaintCtx,
        data: &AppData,
        middlex: f64,
        downy: &mut f64,
        tasks: &[usize],
    ) {
        let time = data.slider * data.total_time;

        let tasks_limit = match data.tasks_limit_str.parse::<usize>() {
            Ok(x) => x,
            Err(_) => return,
        };

        let leftx = middlex - BLOCK_WIDTH / 2.;
        let rightx = middlex + BLOCK_WIDTH / 2.;

        // title for tasks and border around it
        paint_text(ctx, "Tasks", 18., Point::new(leftx + 5., *downy), false, false);
        ctx.stroke(
            Rect::from_points(Point::new(leftx, *downy), Point::new(leftx + LABEL_WIDTH, *downy + 25.)),
            &Color::WHITE,
            1.,
        );

        *downy += 25.;

        let mut active_tasks: Vec<usize> = Vec::new();

        for &task_id in tasks.iter() {
            if let Some(task_info) = &data.task_info.borrow()[task_id] {
                if time < task_info.scheduled || task_info.completed < time {
                    continue;
                }

                active_tasks.push(task_id);
            }
        }

        let extra_tasks = active_tasks.len().max(tasks_limit) - tasks_limit;
        let extra_tasks_space = tasks_limit - active_tasks.len().min(tasks_limit);
        if extra_tasks > 0 {
            paint_text(
                ctx,
                &format!("+{}", extra_tasks),
                18.,
                Point::new(leftx + LABEL_WIDTH + 5., *downy - 25.),
                false,
                false,
            );
        }

        for task_id in active_tasks.into_iter().rev().take(tasks_limit).rev() {
            let task_info = data.task_info.borrow()[task_id].as_ref().unwrap().clone();
            let task = &data.graph.borrow().tasks[task_id];

            // task name
            paint_text(ctx, &task.name, 15., Point::new(leftx + 5., *downy), false, false);

            // task status, either pending or progress bar
            if time < task_info.started {
                paint_colored_text(
                    ctx,
                    "pending...",
                    15.,
                    Point::new(middlex + 5., *downy),
                    false,
                    false,
                    task_info.get_color(data),
                );
            } else {
                let mut width = BLOCK_WIDTH / 2. - 10.;
                width *= (time - task_info.started) / (task_info.completed - task_info.started);
                ctx.fill(
                    Rect::from_points(
                        Point::new(middlex + 5., *downy + ROW_STEP / 2. + 2.5 + CORE_SIZE / 2.),
                        Point::new(middlex + 5. + width, *downy + ROW_STEP / 2. + 2.5 - CORE_SIZE / 2.),
                    ),
                    &task_info.get_color(data),
                );
                ctx.stroke(
                    Rect::from_points(
                        Point::new(middlex + 5., *downy + ROW_STEP / 2. + 2.5 + CORE_SIZE / 2.),
                        Point::new(rightx - 5., *downy + ROW_STEP / 2. + 2.5 - CORE_SIZE / 2.),
                    ),
                    &Color::WHITE,
                    1.,
                );
            }

            *downy += ROW_STEP;
        }

        *downy += ROW_STEP * extra_tasks_space as f64;
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

        paint_text(ctx, name, 25., Point::new(middlex, upy), true, false);

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
            paint_text(
                ctx,
                &format!("Cores: {}", compute.cores),
                18.,
                Point::new(leftx + 5., downy),
                false,
                false,
            );
            paint_text(
                ctx,
                &format!("Speed: {}", compute.speed),
                18.,
                Point::new(leftx + 5. + LABEL_WIDTH, downy),
                false,
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
            let mut ycore = downy + 25. / 2.;
            let mut core_index = 0;

            for &task_id in compute.tasks.iter() {
                if data.task_info.borrow()[task_id].is_none() {
                    continue;
                }
                let task_info = data.task_info.borrow()[task_id].as_ref().unwrap().clone();
                if time < task_info.scheduled || task_info.completed < time {
                    continue;
                }

                // paint cores
                for _ in 0..task_info.cores {
                    ctx.fill(
                        Rect::from_center_size(Point::new(xcore, ycore), Size::new(CORE_SIZE, CORE_SIZE)),
                        &task_info.get_color(data),
                    );
                    xcore += 20.;
                    core_index += 1;
                    if core_index == CORES_PER_ROW {
                        core_index = 0;
                        xcore -= 20. * CORES_PER_ROW as f64;
                        ycore += 20.;
                    }
                }
            }

            let xcore = leftx + 5. + CORE_SIZE / 2.;
            let ycore = downy + 25. / 2.;

            // cores (without filling them right now)
            for i in 0..compute.cores {
                ctx.stroke(
                    Rect::from_center_size(
                        Point::new(
                            xcore + (i % CORES_PER_ROW) as f64 * 20.,
                            ycore + (i / CORES_PER_ROW) as f64 * 20.,
                        ),
                        Size::new(15., 15.),
                    ),
                    &Color::WHITE,
                    1.,
                );
            }

            downy += 5. + (compute.cores.max(1).div_ceil(CORES_PER_ROW)) as f64 * 20.;

            // line below cores
            ctx.stroke(
                Line::new(Point::new(leftx, downy), Point::new(rightx, downy)),
                &Color::WHITE,
                1.,
            );

            // block with memory
            paint_text(
                ctx,
                &format!("Memory: {}", compute.memory),
                18.,
                Point::new(leftx + 5., downy),
                false,
                false,
            );
            ctx.stroke(
                Rect::from_points(
                    Point::new(leftx, downy),
                    Point::new(leftx + LABEL_WIDTH * 2., downy + 25.),
                ),
                &Color::WHITE,
                1.,
            );

            downy += 25.;
            downy += 25. / 2.;

            // memory pieces
            let mut memoryx = leftx + 5.;
            for &task_id in compute.tasks.iter() {
                if data.task_info.borrow()[task_id].is_none() {
                    continue;
                }
                let task_info = data.task_info.borrow()[task_id].as_ref().unwrap().clone();
                if time < task_info.scheduled || task_info.completed < time {
                    continue;
                }

                let memory_width =
                    data.graph.borrow().tasks[task_id].memory as f64 / compute.memory as f64 * (BLOCK_WIDTH - 10.);
                ctx.fill(
                    Rect::from_points(
                        Point::new(memoryx, downy - CORE_SIZE / 2.),
                        Point::new(memoryx + memory_width, downy + CORE_SIZE / 2.),
                    ),
                    &task_info.get_color(data),
                );

                memoryx += memory_width;
            }

            ctx.stroke(
                Rect::from_center_size(Point::new(middlex, downy), Size::new(BLOCK_WIDTH - 10., CORE_SIZE)),
                &Color::WHITE,
                1.,
            );

            downy += 25. / 2.;

            // line below memory
            ctx.stroke(
                Line::new(Point::new(leftx, downy), Point::new(rightx, downy)),
                &Color::WHITE,
                1.,
            );

            self.draw_actor_tasks_block(ctx, data, middlex, &mut downy, &compute.tasks);
        }

        // block with files
        if let Some(files) = files {
            self.draw_actor_files_block(ctx, data, name, middlex, &mut downy, files);
        }

        // line below files
        ctx.stroke(
            Rect::from_points(Point::new(leftx, upy), Point::new(rightx, downy)),
            &Color::WHITE,
            1.,
        );
    }

    fn get_actor_height(&self, data: &AppData) -> f64 {
        0.
            + 35.  // title
            + 50.  // cores
            + 50.  // memory
            + match data.tasks_limit_str.parse::<usize>() {
                Ok(x) => x as f64 * ROW_STEP + 30.,
                Err(_) => 0.
            }  // tasks
            + match data.files_limit_str.parse::<usize>() {
                Ok(x) => x as f64 * ROW_STEP + 30.,
                Err(_) => 0.
            } // files
    }

    fn get_actors_per_row(&self, width: f64) -> usize {
        ((width / (BLOCK_WIDTH + BLOCK_X_PADDING * 2.)).floor() as usize).max(1)
    }
}

impl Widget<AppData> for PanelsWidget {
    fn event(&mut self, _: &mut EventCtx, _: &Event, _: &mut AppData, _: &Env) {}

    fn lifecycle(&mut self, _: &mut LifeCycleCtx, _: &LifeCycle, _: &AppData, _: &Env) {}
    fn update(&mut self, ctx: &mut UpdateCtx, _: &AppData, _: &AppData, _: &Env) {
        ctx.request_paint();
    }
    fn layout(&mut self, _: &mut LayoutCtx, bc: &BoxConstraints, data: &AppData, _: &Env) -> druid::Size {
        let actor_height = self.get_actor_height(data);
        let actors_per_row = self.get_actors_per_row(bc.max().width);
        let rows = data.compute.borrow().len().div_ceil(actors_per_row) + 1;
        Size::new(
            bc.max().width,
            rows as f64 * actor_height + (rows + 1) as f64 * BLOCK_Y_PADDING,
        )
    }

    fn paint(&mut self, ctx: &mut PaintCtx, data: &AppData, _: &Env) {
        let size = ctx.size();

        let actor_height = self.get_actor_height(data);
        let actors_per_row = self.get_actors_per_row(size.width).min(data.compute.borrow().len());

        self.draw_actor(
            ctx,
            data,
            Point::new(size.width / 2., BLOCK_Y_PADDING),
            "runner",
            None,
            Some(&data.scheduler_files.borrow()),
        );

        for (i, compute) in (*data.compute.borrow()).iter().enumerate() {
            let row = i / actors_per_row + 1;
            let position = Point::new(
                size.width / (actors_per_row as f64) * ((i % actors_per_row) as f64 + 0.5),
                row as f64 * (actor_height + BLOCK_Y_PADDING) + BLOCK_Y_PADDING,
            );
            self.draw_actor(ctx, data, position, &compute.name, Some(compute), Some(&compute.files));
        }
    }
}

use std::collections::BTreeSet;
use std::fmt::Write;

use druid::piet::{FontFamily, Text, TextLayout, TextLayoutBuilder};
use druid::widget::prelude::*;
use druid::Color;
use druid::Point;

use crate::app_data::AppData;

pub fn paint_colored_text(
    ctx: &mut PaintCtx,
    text: &str,
    font: f64,
    mut pos: Point,
    centerx: bool,
    centery: bool,
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
    if centery {
        pos.y -= text_size.height / 2.;
    }
    ctx.draw_text(&layout, pos);
}

pub fn paint_text(ctx: &mut PaintCtx, text: &str, font: f64, pos: Point, centerx: bool, centery: bool) {
    paint_colored_text(ctx, text, font, pos, centerx, centery, Color::WHITE);
}

pub fn draw_arrow(ctx: &mut PaintCtx, pos: Point, is_download: bool) {
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

pub fn draw_download(ctx: &mut PaintCtx, pos: Point) {
    draw_arrow(ctx, pos, true);
}

pub fn draw_upload(ctx: &mut PaintCtx, pos: Point) {
    draw_arrow(ctx, pos, false);
}

pub fn get_text_task_info(data: &AppData, task_id: usize) -> String {
    if task_id == data.graph.borrow().tasks.len() {
        // input
        let mut inputs: BTreeSet<usize> = BTreeSet::new();
        for task in data.graph.borrow().tasks.iter() {
            for &data_item in task.inputs.iter() {
                inputs.insert(data_item);
            }
        }
        for task in data.graph.borrow().tasks.iter() {
            for data_item in task.outputs.iter() {
                inputs.remove(&data_item);
            }
        }
        return format!(
            "Inputs: {}\n\n",
            inputs
                .iter()
                .map(|&i| data.graph.borrow().data_items[i].name.clone())
                .collect::<Vec<_>>()
                .join(", ")
        );
    } else if task_id == data.graph.borrow().tasks.len() + 1 {
        // output
        let mut outputs: BTreeSet<usize> = BTreeSet::new();
        for task in data.graph.borrow().tasks.iter() {
            for &data_item in task.outputs.iter() {
                outputs.insert(data_item);
            }
        }
        for task in data.graph.borrow().tasks.iter() {
            for data_item in task.inputs.iter() {
                outputs.remove(&data_item);
            }
        }
        return format!(
            "Outputs: {}\n\n",
            outputs
                .iter()
                .map(|&i| data.graph.borrow().data_items[i].name.clone())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    let task_info = &data.task_info.borrow()[task_id];
    let task = &data.graph.borrow().tasks[task_id];

    let mut result = String::new();
    write!(result, "Task: {}\n\n", task.name).unwrap();
    if task_info.is_some() {
        write!(
            result,
            "Total time: {:.3}\n\n",
            task_info.as_ref().unwrap().completed - task_info.as_ref().unwrap().scheduled
        )
        .unwrap();
    }
    let inputs: Vec<String> = task
        .inputs
        .iter()
        .map(|&i| data.graph.borrow().data_items[i].name.clone())
        .collect();
    let outputs: Vec<String> = task
        .outputs
        .iter()
        .map(|&i| data.graph.borrow().data_items[i].name.clone())
        .collect();
    write!(result, "Inputs: {}\n\n", inputs.join(", ")).unwrap();
    write!(result, "Outputs: {}\n\n", outputs.join(", ")).unwrap();
    result
}

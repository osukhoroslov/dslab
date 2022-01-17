use std::env;

use druid::widget::{CrossAxisAlignment, Flex, Label, Scroll, SizedBox, Slider, TextBox, Widget};
use druid::Color;
use druid::{AppLauncher, Size, WidgetExt, WindowDesc};

mod data;
mod poly;
use crate::data::*;
mod app_data;
use crate::app_data::AppData;
mod drawing_widget;
use crate::drawing_widget::DrawingWidget;

pub const PADDING: f64 = 8.0;

fn main() {
    let trace_log: TraceLog = serde_json::from_str(
        &std::fs::read_to_string(match env::args().collect::<Vec<_>>().get(1) {
            Some(x) => x.clone(),
            None => {
                eprintln!("Usage: cargo run -- /path/to/trace.json");
                std::process::exit(1);
            }
        })
        .unwrap(),
    )
    .unwrap();

    let app_data = AppData::from_trace_log(trace_log);

    let window = WindowDesc::new(make_layout)
        .window_size(Size {
            width: 1200.0,
            height: 900.0,
        })
        .resizable(true)
        .title("Workflow");
    AppLauncher::with_window(window)
        .launch(app_data)
        .expect("launch failed");
}

fn make_layout() -> impl Widget<AppData> {
    Flex::column()
        .with_flex_child(
            Flex::row()
                .with_child(
                    Flex::column()
                        .with_child(
                            Flex::row()
                                .with_child(Label::new("Files limit: "))
                                .with_spacer(PADDING)
                                .with_child(TextBox::new().lens(AppData::files_limit_str)),
                        )
                        .with_spacer(PADDING)
                        .with_child(
                            Flex::row()
                                .with_child(Label::new("Tasks limit: "))
                                .with_spacer(PADDING)
                                .with_child(TextBox::new().lens(AppData::tasks_limit_str)),
                        )
                        .cross_axis_alignment(CrossAxisAlignment::End)
                        .expand_height()
                        .padding(PADDING)
                        .border(Color::WHITE, 1.),
                )
                .with_spacer(PADDING)
                .with_flex_child(
                    Scroll::new(DrawingWidget {})
                        .vertical()
                        .padding(PADDING)
                        .border(Color::WHITE, 1.)
                        .expand_height(),
                    1.,
                )
                .cross_axis_alignment(CrossAxisAlignment::Start),
            1.,
        )
        .with_spacer(PADDING)
        .with_child(
            SizedBox::new(Slider::new().lens(AppData::slider))
                .expand_width()
                .padding(PADDING)
                .border(Color::WHITE, 1.),
        )
        .cross_axis_alignment(CrossAxisAlignment::Start)
        .padding(PADDING)
}

mod app_data;
mod data;
mod draw_utils;
mod graph_widget;
mod panels_widget;
mod poly;
mod timeline_widget;

use std::path::PathBuf;

use clap::Parser;
use druid::kurbo::Insets;
use druid::widget::{
    Axis, Checkbox, CrossAxisAlignment, Flex, Label, LineBreaking, Scroll, Slider, Tabs, TabsEdge, TabsTransition,
    TextBox, Widget,
};
use druid::Color;
use druid::{AppLauncher, Size, WidgetExt, WindowDesc};

use dslab_dag::trace_log::TraceLog;

use crate::app_data::{AppData, AppDataSettings};
use crate::draw_utils::*;
use crate::graph_widget::GraphWidget;
use crate::panels_widget::PanelsWidget;
use crate::timeline_widget::TimelineWidget;

pub const PADDING: f64 = 8.0;

#[derive(Parser)]
struct Args {
    /// Path to json file with traces.
    #[arg(short, long)]
    trace: PathBuf,

    /// Path to json file with viewer settings to overwrite default ones.
    #[arg(short, long)]
    settings: Option<PathBuf>,
}

fn main() {
    let args = Args::parse();
    let trace_log: TraceLog = serde_json::from_str(
        &std::fs::read_to_string(&args.trace).unwrap_or_else(|_| panic!("Can't read trace from {:?}", &args.trace)),
    )
    .unwrap_or_else(|_| panic!("Can't parse trace from {:?}", &args.trace));
    let settings: Option<AppDataSettings> = args.settings.map(|s| {
        serde_json::from_str(
            &std::fs::read_to_string(&s).unwrap_or_else(|_| panic!("Can't read settings from {:?}", &s)),
        )
        .unwrap_or_else(|_| panic!("Can't parse settings from {:?}", &args.trace))
    });

    let mut app_data = AppData::from_trace_log(trace_log);
    if let Some(settings) = settings {
        app_data.apply_settings(&settings);
    }

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
    let get_task_info_label = || {
        Scroll::new(
            Label::new(|data: &AppData, _env: &_| match data.selected_task {
                Some(x) => get_text_task_info(data, x),
                None => String::new(),
            })
            .with_line_break_mode(LineBreaking::WordWrap),
        )
        .vertical()
        .expand_height()
        .expand_width()
        .padding(PADDING)
        .border(Color::WHITE, 1.)
    };

    Flex::column()
        .with_flex_child(
            Tabs::new()
                .with_axis(Axis::Horizontal)
                .with_edge(TabsEdge::Leading)
                .with_transition(TabsTransition::Instant)
                .with_tab(
                    "Panels",
                    Flex::row()
                        .with_child(
                            Flex::column()
                                .with_flex_child(
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
                                    1.,
                                )
                                .with_spacer(PADDING)
                                .with_flex_child(get_task_info_label(), 1.)
                                .fix_width(200.),
                        )
                        .with_spacer(PADDING)
                        .with_flex_child(
                            Scroll::new(PanelsWidget {})
                                .vertical()
                                .padding(PADDING)
                                .border(Color::WHITE, 1.)
                                .expand_height(),
                            1.,
                        )
                        .cross_axis_alignment(CrossAxisAlignment::Start),
                )
                .with_tab(
                    "Timeline",
                    Flex::row()
                        .with_child(
                            Flex::column()
                                .with_flex_child(
                                    Flex::column()
                                        .with_child(Checkbox::new("Downloading").lens(AppData::timeline_downloading))
                                        .with_spacer(PADDING)
                                        .with_child(Checkbox::new("Cores").lens(AppData::timeline_cores))
                                        .with_spacer(PADDING)
                                        .with_child(Checkbox::new("Memory").lens(AppData::timeline_memory))
                                        .with_spacer(PADDING)
                                        .with_child(Checkbox::new("Uploading").lens(AppData::timeline_uploading))
                                        .cross_axis_alignment(CrossAxisAlignment::Start)
                                        .expand_width()
                                        .expand_height()
                                        .padding(PADDING)
                                        .border(Color::WHITE, 1.),
                                    1.,
                                )
                                .with_spacer(PADDING)
                                .with_flex_child(get_task_info_label(), 1.)
                                .fix_width(200.),
                        )
                        .with_spacer(PADDING)
                        .with_flex_child(
                            Scroll::new(TimelineWidget::new())
                                .vertical()
                                .padding(PADDING)
                                .border(Color::WHITE, 1.)
                                .expand_height(),
                            1.,
                        )
                        .cross_axis_alignment(CrossAxisAlignment::Start),
                )
                .with_tab(
                    "Graph",
                    Flex::row()
                        .with_child(
                            Flex::column()
                                .with_flex_child(
                                    Flex::column()
                                        .with_child(
                                            Checkbox::new("Levels from the end").lens(AppData::graph_levels_from_end),
                                        )
                                        .with_spacer(PADDING)
                                        .with_child(
                                            Checkbox::new("Variable edge width")
                                                .lens(AppData::graph_variable_edge_width),
                                        )
                                        .with_spacer(PADDING)
                                        .with_child(
                                            Checkbox::new("Variable node size").lens(AppData::graph_variable_node_size),
                                        )
                                        .cross_axis_alignment(CrossAxisAlignment::Start)
                                        .expand_width()
                                        .expand_height()
                                        .padding(PADDING)
                                        .border(Color::WHITE, 1.),
                                    1.,
                                )
                                .with_spacer(PADDING)
                                .with_flex_child(get_task_info_label(), 1.)
                                .fix_width(200.),
                        )
                        .with_spacer(PADDING)
                        .with_flex_child(GraphWidget::new().padding(PADDING).border(Color::WHITE, 1.), 1.)
                        .cross_axis_alignment(CrossAxisAlignment::Start),
                ),
            1.,
        )
        .with_child(
            Flex::row()
                .with_child(
                    Label::new(|data: &AppData, _env: &_| format!("Time: {:.3}", data.slider * data.total_time))
                        .padding(PADDING)
                        .border(Color::WHITE, 1.)
                        .fix_width(200.),
                )
                .with_spacer(PADDING)
                .with_flex_child(
                    Slider::new()
                        .lens(AppData::slider)
                        .padding(Insets::uniform_xy(8., 9.))
                        .border(Color::WHITE, 1.)
                        .expand_width(),
                    1.,
                )
                .padding(Insets::new(5.5, 4., 5., 4.))
                .expand_width(),
        )
        .cross_axis_alignment(CrossAxisAlignment::Start)
        .padding(PADDING)
}

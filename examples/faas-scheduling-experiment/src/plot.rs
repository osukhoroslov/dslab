use std::iter::zip;

use plotters::prelude::*;

const METRICS: &[&str] = &["99% relative slowdown", "cold start fraction (%)"];
const PLOT_LIMIT: [f64; 2] = [80., 10.];

pub(crate) fn plot_results(plot: &str, labels: &[String], rps: &[f64], points: &[Vec<[f64; 2]>]) {
    let mut styles = Vec::with_capacity(labels.len());
    for i in 0..labels.len() {
        styles.push(Into::<ShapeStyle>::into(Palette99::pick(i)).filled());
    }
    let root_area = BitMapBackend::new(plot, (1600, 900)).into_drawing_area();
    root_area.fill(&WHITE).unwrap();
    let tmp = root_area.split_vertically((50).percent());
    let areas: [_; 2] = [tmp.0, tmp.1];
    for idx in 0..2 {
        let max = PLOT_LIMIT[idx];
        let mut ctx = ChartBuilder::on(&areas[idx])
            .margin(20)
            .set_label_area_size(LabelAreaPosition::Left, 60)
            .set_label_area_size(LabelAreaPosition::Bottom, 40)
            .build_cartesian_2d(rps[0]..rps.last().copied().unwrap(), 0.0..max)
            .unwrap();
        ctx.configure_mesh()
            .y_desc(METRICS[idx])
            .x_desc("requests per second")
            .label_style(("sans-serif", 20))
            .draw()
            .unwrap();
        for (i, pts) in points.iter().enumerate() {
            let style = styles[i];
            let mut series = Vec::new();
            let mut begin = Vec::new();
            let mut last = (f64::NAN, f64::NAN);
            let mut end = Vec::new();
            let mut vec = Vec::new();
            for (x, y) in zip(rps.iter(), pts.iter()) {
                if y[idx] > max {
                    if !vec.is_empty() {
                        end.push((*x, y[idx]));
                        series.push(vec);
                    }
                    vec = Vec::new();
                    last = (*x, y[idx]);
                } else {
                    if vec.is_empty() {
                        begin.push(last);
                    }
                    vec.push((*x, y[idx]));
                }
            }
            if !vec.is_empty() {
                end.push((f64::NAN, f64::NAN));
                series.push(vec);
            }
            if !series.is_empty() {
                ctx.draw_series(LineSeries::new(series[0].iter().copied(), style).point_size(5))
                    .unwrap()
                    .label(labels[i].clone())
                    .legend(move |pos| Circle::new(pos, 5, style));
                for s in &series[1..] {
                    ctx.draw_series(LineSeries::new(s.iter().copied(), style).point_size(5))
                        .unwrap();
                }
                let find_intersection = |x1, y1, x2, y2, y| {
                    let dx = x2 - x1;
                    let dy = y2 - y1;
                    (y - y1 + x1 * dy / dx) * dx / dy
                };
                for i in 0..series.len() {
                    if !begin[i].0.is_nan() {
                        let x = find_intersection(begin[i].0, begin[i].1, series[i][0].0, series[i][0].1, max);
                        let tmp = vec![(x, max), series[i][0]];
                        ctx.draw_series(LineSeries::new(tmp, style)).unwrap();
                    }
                }
                for i in 0..series.len() {
                    if !end[i].0.is_nan() {
                        let pt = *series[i].last().unwrap();
                        let x = find_intersection(pt.0, pt.1, end[i].0, end[i].1, max);
                        let tmp = vec![pt, (x, max)];
                        ctx.draw_series(LineSeries::new(tmp, style)).unwrap();
                    }
                }
            }
        }
        ctx.configure_series_labels()
            .position(if idx == 0 {
                SeriesLabelPosition::UpperLeft
            } else {
                SeriesLabelPosition::LowerRight
            })
            .border_style(BLACK)
            .background_style(WHITE.mix(0.4))
            .label_font(("sans-serif", 20))
            .draw()
            .unwrap();
    }
}

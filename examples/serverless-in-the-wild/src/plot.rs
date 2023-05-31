use plotters::prelude::*;

pub(crate) fn plot_metrics(plot: &str, mut labels: Vec<String>, mut points: Vec<(f64, f64)>) {
    if let Some(ban) = labels.iter().position(|x| x.contains("unloading")) {
        // can't plot no unloading policy because of infinite wasted memory
        labels.remove(ban);
        points.remove(ban);
    }
    let max_cold_start = 1.01 * points.iter().fold(0., |acc, x| f64::max(x.0, acc));
    let min_wasted_mem = points.iter().fold(f64::MAX, |acc, x| f64::min(x.1, acc)) - 1.;
    let max_wasted_mem = points.iter().fold(0., |acc, x| f64::max(x.1, acc)) + 1.;
    let root_area = BitMapBackend::new(plot, (1600, 900)).into_drawing_area();
    root_area.fill(&WHITE).unwrap();
    let mut ctx = ChartBuilder::on(&root_area)
        .set_label_area_size(LabelAreaPosition::Left, 70)
        .set_label_area_size(LabelAreaPosition::Bottom, 50)
        .build_cartesian_2d(0. ..max_cold_start, min_wasted_mem..max_wasted_mem)
        .unwrap();
    ctx.configure_mesh()
        .y_desc("normalized wasted memory time (%)")
        .x_desc("3rd quartile app cold start percentage (%)")
        .label_style(("sans-serif", 20))
        .draw()
        .unwrap();
    for i in 0..labels.len() {
        if labels[i].contains("unloading") {
            continue;
        } else if labels[i].contains("keepalive") {
            ctx.draw_series([TriangleMarker::new(points[i], 8, Palette99::pick(i))])
                .unwrap()
                .label(labels[i].clone())
                .legend(move |pos| TriangleMarker::new(pos, 8, Palette99::pick(i)));
        } else {
            ctx.draw_series([Circle::new(
                points[i],
                8,
                Into::<ShapeStyle>::into(Palette99::pick(i)).filled(),
            )])
            .unwrap()
            .label(labels[i].clone())
            .legend(move |pos| Circle::new(pos, 8, Into::<ShapeStyle>::into(Palette99::pick(i)).filled()));
        }
    }
    ctx.configure_series_labels()
        .label_font(("sans-serif", 20))
        .border_style(BLACK)
        .background_style(WHITE.mix(0.8))
        .draw()
        .unwrap();
}

pub(crate) fn plot_cdf(plot: &str, labels: Vec<String>, mut data: Vec<Vec<f64>>) {
    let root_area = BitMapBackend::new(plot, (1600, 900)).into_drawing_area();
    root_area.fill(&WHITE).unwrap();
    let mut ctx = ChartBuilder::on(&root_area)
        .set_label_area_size(LabelAreaPosition::Left, 50)
        .set_label_area_size(LabelAreaPosition::Bottom, 50)
        .build_cartesian_2d(0f64..101.5f64, 0f64..1.01f64)
        .unwrap();
    ctx.configure_mesh()
        .y_desc("CDF")
        .x_desc("app cold start percentage (%)")
        .label_style(("sans-serif", 20))
        .draw()
        .unwrap();
    for i in 0..labels.len() {
        data[i].sort_by(|a, b| a.total_cmp(b));
        let mut pts = Vec::new();
        pts.push((0., 0.));
        let n = data[i].len() as f64;
        for (j, p) in data[i].iter().copied().enumerate() {
            pts.push((p, (j as f64) / n));
            pts.push((p, ((j + 1) as f64) / n));
        }
        pts.push((100., 1.));
        if labels[i].contains("unloading") || labels[i].contains("keepalive") {
            ctx.draw_series(LineSeries::new(
                pts,
                Into::<ShapeStyle>::into(Palette99::pick(i)).filled(),
            ))
            .unwrap()
            .label(labels[i].clone())
            .legend(move |pos| TriangleMarker::new(pos, 8, Palette99::pick(i)));
        } else {
            ctx.draw_series(LineSeries::new(
                pts,
                Into::<ShapeStyle>::into(Palette99::pick(i)).filled(),
            ))
            .unwrap()
            .label(labels[i].clone())
            .legend(move |pos| Circle::new(pos, 8, Into::<ShapeStyle>::into(Palette99::pick(i)).filled()));
        }
    }
    ctx.configure_series_labels()
        .label_font(("sans-serif", 20))
        .border_style(BLACK)
        .background_style(WHITE.mix(0.8))
        .draw()
        .unwrap();
}

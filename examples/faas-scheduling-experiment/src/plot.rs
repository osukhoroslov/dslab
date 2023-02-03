use std::iter::zip;

use plotters::prelude::*;

const METRICS: &[&str] = &["99% relative slowdown", "cold start fraction (%)"];

pub(crate) fn plot_results(plot: &str, labels: &[String], rps: &[f64], points: &[Vec<[f64; 2]>]) {
    let side = (labels.len() as f64).cbrt().ceil() as usize;
    let step = 200 / side;
    let mut styles = Vec::with_capacity(labels.len());
    for i in 0..labels.len() {
        let r = ((i % side) * step + 20) as u8;
        let tmp = i / side;
        let g = ((tmp % side) * step + 20) as u8;
        let b = ((tmp / side) * step + 20) as u8;
        styles.push(Into::<ShapeStyle>::into(RGBColor(r, g, b)).filled());
    }
    let root_area = BitMapBackend::new(plot, (1600, 900)).into_drawing_area();
    root_area.fill(&WHITE).unwrap();
    let tmp = root_area.split_vertically((50).percent());
    let areas: [_; 2] = [tmp.0, tmp.1];
    for idx in 0..2 {
        let max = points
            .iter()
            .map(|v| v.iter().fold(0., |acc, x| f64::max(acc, x[idx])))
            .fold(0., f64::max)
            * 1.1;
        /*let repr = points
        .iter()
        .map(|v| v.iter().fold(0., |acc, x| f64::max(acc, x[idx])))
        .fold(f64::MAX, f64::min)
        * 2.;*/
        let repr = 100.;
        let mut ctx = ChartBuilder::on(&areas[idx])
            .set_label_area_size(LabelAreaPosition::Left, 60)
            .set_label_area_size(LabelAreaPosition::Bottom, 60)
            .build_cartesian_2d(rps[0]..rps.last().copied().unwrap(), 0.0..f64::min(max, repr))
            .unwrap();
        ctx.configure_mesh()
            .y_desc(METRICS[idx])
            .x_desc("requests per second")
            .draw()
            .unwrap();
        for (i, pts) in points.iter().enumerate() {
            let style = styles[i];
            ctx.draw_series(
                LineSeries::new(zip(rps.iter(), pts.iter()).map(|(x, y)| (*x, y[idx])), style).point_size(5),
            )
            .unwrap()
            .label(labels[i].clone())
            .legend(move |pos| Circle::new(pos, 5, style));
        }
        ctx.configure_series_labels()
            .position(SeriesLabelPosition::UpperLeft)
            .border_style(BLACK)
            .background_style(WHITE.mix(0.8))
            .draw()
            .unwrap();
    }
}

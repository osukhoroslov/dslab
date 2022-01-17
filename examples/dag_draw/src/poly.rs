use druid::piet::kurbo::PathEl;
use druid::piet::kurbo::Shape;

use druid::{Point, Rect};

#[derive(Clone)]
pub struct Poly {
    pub points: Vec<Point>,
}

impl Poly {
    pub fn from_vec(points: Vec<Point>) -> Self {
        Poly { points }
    }

    fn triangle_area(&self, a: Point, b: Point, c: Point) -> f64 {
        (b.x - a.x) * (c.y - a.y) + (b.y - a.y) * (c.x - a.x)
    }
}

pub struct PolyPathIter {
    poly: Poly,
    ind: usize,
}

impl Iterator for PolyPathIter {
    type Item = PathEl;

    fn next(&mut self) -> Option<PathEl> {
        self.ind += 1;
        if self.ind == 1 {
            Some(PathEl::MoveTo(self.poly.points[0]))
        } else if self.ind <= self.poly.points.len() {
            Some(PathEl::LineTo(self.poly.points[self.ind - 1]))
        } else {
            None
        }
    }
}

impl Shape for Poly {
    type PathElementsIter = PolyPathIter;

    fn path_elements(&self, _tolerance: f64) -> PolyPathIter {
        PolyPathIter {
            poly: self.clone(),
            ind: 0,
        }
    }

    fn area(&self) -> f64 {
        let mut res = 0.0;

        for i in 1..self.points.len() {
            res += self.triangle_area(self.points[0], self.points[i - 1], self.points[i]);
        }

        res.abs()
    }

    fn perimeter(&self, _tolerance: f64) -> f64 {
        let mut res = 0.0;

        for i in 1..self.points.len() {
            res += self.points[i - 1].distance(self.points[i]);
        }

        res
    }

    fn bounding_box(&self) -> druid::Rect {
        let mut left = self.points[0].x;
        let mut right = self.points[0].x;
        let mut up = self.points[0].y;
        let mut down = self.points[0].y;

        for point in self.points.iter() {
            left = left.min(point.x);
            right = right.max(point.x);
            down = down.min(point.y);
            up = up.max(point.y);
        }

        Rect::new(left, down, right, up)
    }

    fn winding(&self, _p: Point) -> i32 {
        unimplemented!()
    }
}

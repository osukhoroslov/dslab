use rand::rngs::ThreadRng;
use rand::Rng;

struct Node {
    value: i64,
    mx: i64,
    md: i64,
    l: f64,
    r: f64,
    sl: f64,
    sr: f64,
    priority: u64,
    left: Option<Box<Node>>,
    right: Option<Box<Node>>,
}

impl Node {
    fn new(value: i64, l: f64, r: f64, priority: u64) -> Self {
        Self {
            value,
            mx: value,
            md: 0,
            l,
            r,
            sl: l,
            sr: r,
            priority,
            left: None,
            right: None,
        }
    }

    fn modify(&mut self, m: i64) {
        self.value += m;
        self.mx += m;
        self.md += m;
    }

    fn update(&mut self) {
        self.mx = self
            .value
            .max(self.left.as_ref().map(|i| i.mx).unwrap_or(0))
            .max(self.right.as_ref().map(|i| i.mx).unwrap_or(0));
        self.sl = self.left.as_ref().map(|i| i.sl).unwrap_or(self.l);
        self.sr = self.right.as_ref().map(|i| i.sr).unwrap_or(self.r);
    }

    fn push(&mut self) {
        if let Some(ref mut left) = &mut self.left {
            left.modify(self.md);
        }
        if let Some(ref mut right) = &mut self.right {
            right.modify(self.md);
        }
        self.md = 0;
    }
}

impl Node {
    fn merge(mut left: Option<Box<Self>>, mut right: Option<Box<Self>>) -> Option<Box<Self>> {
        if left.is_none() {
            right
        } else if right.is_none() {
            left
        } else if left.as_ref().unwrap().priority < right.as_ref().unwrap().priority {
            left.as_mut().unwrap().push();
            let m = left.as_mut().unwrap().right.take();
            left.as_mut().unwrap().right = Self::merge(m, right);
            left.as_mut().unwrap().update();
            left
        } else {
            right.as_mut().unwrap().push();
            let m = right.as_mut().unwrap().left.take();
            right.as_mut().unwrap().left = Self::merge(left, m);
            right.as_mut().unwrap().update();
            right
        }
    }

    fn split_at(mut root: Option<Box<Self>>, m: f64, rand: &mut ThreadRng) -> (Option<Box<Self>>, Option<Box<Self>>) {
        if root.is_none() {
            return (None, None);
        }
        root.as_mut().unwrap().push();
        if root.as_ref().unwrap().r <= m {
            let (a, b) = Self::split_at(root.as_mut().unwrap().right.take(), m, rand);
            root.as_mut().unwrap().right = a;
            root.as_mut().unwrap().update();
            (root, b)
        } else if m <= root.as_ref().unwrap().l {
            let (a, b) = Self::split_at(root.as_mut().unwrap().left.take(), m, rand);
            root.as_mut().unwrap().left = b;
            root.as_mut().unwrap().update();
            (a, root)
        } else {
            let root = root.as_mut().unwrap();
            let mut left = Box::new(Node::new(root.value, root.l, m, rand.gen()));
            let mut right = Box::new(Node::new(root.value, m, root.r, rand.gen()));
            left.left = root.left.take();
            right.right = root.right.take();
            left.update();
            right.update();
            (Some(left), Some(right))
        }
    }

    fn max(&mut self, l: f64, r: f64) -> i64 {
        if self.sr <= l || self.sl >= r {
            0
        } else if l <= self.sl && self.sr <= r {
            self.mx
        } else {
            self.push();
            let mut result = self
                .left
                .as_mut()
                .map(|i| i.max(l, r))
                .unwrap_or(0)
                .max(self.right.as_mut().map(|i| i.max(l, r)).unwrap_or(0));
            if !(r <= self.l || l >= self.r) {
                result = result.max(self.value);
            }
            result
        }
    }
}

pub struct Treap {
    root: Option<Box<Node>>,
    rand: ThreadRng,
}

impl Treap {
    pub fn new() -> Self {
        let mut rand = rand::thread_rng();
        let priority = rand.gen();
        Self {
            root: Some(Box::new(Node::new(0, f64::MIN, f64::MAX, priority))),
            rand,
        }
    }

    pub fn add(&mut self, l: f64, r: f64, value: i64) {
        if l >= r {
            return;
        }
        let (p1, p23) = Node::split_at(self.root.take(), l, &mut self.rand);
        let (mut p2, p3) = Node::split_at(p23, r, &mut self.rand);
        p2.as_mut().unwrap().modify(value);
        self.root = Node::merge(Node::merge(p1, p2), p3);
    }

    pub fn max(&mut self, l: f64, r: f64) -> i64 {
        if l >= r {
            return 0;
        }
        self.root.as_mut().unwrap().max(l, r)
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;
    use rand::SeedableRng;

    use crate::schedulers::treap::Treap;

    #[test]
    fn random() {
        let mut rand = rand::rngs::SmallRng::seed_from_u64(42);
        let mut f64_values = (0..100).map(|_| rand.gen::<f64>() * 1000.).collect::<Vec<_>>();
        f64_values.sort_by(|a, b| a.total_cmp(b));

        let mut treap = Treap::new();
        let mut segs: Vec<(f64, f64, i64)> = Vec::new();
        segs.push((f64::MIN, f64::MAX, 0));

        for _ in 0..10000 {
            let mut l = f64_values[rand.gen::<usize>() % f64_values.len()];
            let mut r = f64_values[rand.gen::<usize>() % f64_values.len()];
            if l > r {
                std::mem::swap(&mut l, &mut r);
            }
            if l == r {
                continue;
            }

            let treap_max = treap.max(l, r);
            let mut segs_max = 0;
            for &(sl, sr, x) in segs.iter() {
                if sr <= l || sl >= r {
                    continue;
                }
                segs_max = segs_max.max(x);
            }

            assert_eq!(segs_max, treap_max);

            let md = (rand.gen::<u64>() % 100) as i64;

            treap.add(l, r, md);

            let mut new_segs = Vec::new();
            for (sl, sr, x) in segs {
                // don't intersect
                if sr <= l || sl >= r {
                    new_segs.push((sl, sr, x));
                    continue;
                }

                if l <= sl && sr <= r {
                    // [sl, sr] inside [l, r]
                    new_segs.push((sl, sr, x + md));
                } else if sl <= l && r <= sr {
                    // [l, r] inside [sl, sr]
                    if sl != l {
                        new_segs.push((sl, l, x));
                    }
                    if r != sr {
                        new_segs.push((r, sr, x));
                    }
                    new_segs.push((l, r, x + md));
                } else if sl <= l && sr <= r {
                    // intersect
                    if sl != l {
                        new_segs.push((sl, l, x));
                    }
                    new_segs.push((l, sr, x + md));
                } else if l <= sl && r <= sr {
                    // intersect
                    new_segs.push((sl, r, x + md));
                    if r != sr {
                        new_segs.push((r, sr, x));
                    }
                } else {
                    unreachable!();
                }
            }

            segs = new_segs;
        }
    }
}

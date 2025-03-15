use std::cell::Cell;

use rand::rngs::ThreadRng;
use rand::Rng;

#[derive(Clone)]
struct Node {
    value: u64,
    mx: u64,
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
    fn new(value: u64, l: f64, r: f64, priority: u64) -> Self {
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
        self.value = (self.value as i64 + m) as u64;
        self.mx = (self.mx as i64 + m) as u64;
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

    fn max(&mut self, l: f64, r: f64) -> u64 {
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

    fn nodes(&self) -> usize {
        1 + self.left.as_ref().map(|i| i.nodes()).unwrap_or(0) + self.right.as_ref().map(|i| i.nodes()).unwrap_or(0)
    }

    fn height(&self) -> usize {
        1 + self
            .left
            .as_ref()
            .map(|i| i.height())
            .unwrap_or(0)
            .max(self.right.as_ref().map(|i| i.height()).unwrap_or(0))
    }
}

pub struct Treap {
    root: Cell<Option<Box<Node>>>,
    rand: ThreadRng,
}

impl Treap {
    pub fn new() -> Self {
        let mut rand = rand::thread_rng();
        let priority = rand.gen();
        Self {
            root: Cell::new(Some(Box::new(Node::new(0, f64::MIN, f64::MAX, priority)))),
            rand,
        }
    }

    pub fn add_i64(&mut self, l: f64, r: f64, value: i64) {
        if l >= r {
            return;
        }
        let (p1, p23) = Node::split_at(self.root.get_mut().take(), l, &mut self.rand);
        let (mut p2, p3) = Node::split_at(p23, r, &mut self.rand);
        p2.as_mut().unwrap().modify(value);
        self.root.set(Node::merge(Node::merge(p1, p2), p3));
    }

    pub fn add(&mut self, l: f64, r: f64, value: u64) {
        self.add_i64(l, r, value as i64);
    }

    pub fn remove(&mut self, l: f64, r: f64, value: u64) {
        self.add_i64(l, r, -(value as i64));
    }

    pub fn max(&self, l: f64, r: f64) -> u64 {
        if l >= r {
            return 0;
        }
        let mut root = self.root.take();
        let result = root.as_mut().unwrap().max(l, r);
        self.root.set(root);
        result
    }

    #[allow(dead_code)]
    fn nodes(&self) -> usize {
        let root = self.root.take();
        let result = root.as_ref().unwrap().nodes();
        self.root.set(root);
        result
    }

    #[allow(dead_code)]
    fn height(&self) -> usize {
        let root = self.root.take();
        let result = root.as_ref().unwrap().height();
        self.root.set(root);
        result
    }
}

impl Default for Treap {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for Treap {
    fn clone(&self) -> Self {
        let root = self.root.take();
        let new = Self {
            root: Cell::new(root.clone()),
            rand: self.rand.clone(),
        };
        self.root.set(root);
        new
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;
    use rand::SeedableRng;
    use rand_pcg::Pcg64;

    use crate::schedulers::treap::Treap;

    #[test]
    fn random() {
        let mut rand = Pcg64::seed_from_u64(42);
        let mut f64_values = (0..100).map(|_| rand.gen::<f64>() * 1000.).collect::<Vec<_>>();
        f64_values.sort_by(|a, b| a.total_cmp(b));

        let mut treap = Treap::new();
        let mut segs: Vec<(f64, f64, u64)> = Vec::new();
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

            let mut segs_min = i64::MAX as u64;
            for &(sl, sr, x) in segs.iter() {
                if sr <= l || sl >= r {
                    continue;
                }
                segs_min = segs_min.min(x);
            }

            assert_eq!(segs_max, treap_max);

            let md = if rand.gen::<bool>() && segs_min > 0 {
                -((rand.gen::<u64>() % (segs_min + 1)) as i64)
            } else {
                (rand.gen::<u64>() % 100) as i64
            };

            let mut new_segs = Vec::new();
            for (sl, sr, x) in segs {
                // don't intersect
                if sr <= l || sl >= r {
                    new_segs.push((sl, sr, x));
                    continue;
                }

                if l <= sl && sr <= r {
                    // [sl, sr] inside [l, r]
                    new_segs.push((sl, sr, ((x as i64) + md) as u64));
                } else if sl <= l && r <= sr {
                    // [l, r] inside [sl, sr]
                    if sl != l {
                        new_segs.push((sl, l, x));
                    }
                    if r != sr {
                        new_segs.push((r, sr, x));
                    }
                    new_segs.push((l, r, ((x as i64) + md) as u64));
                } else if sl <= l && sr <= r {
                    // intersect
                    if sl != l {
                        new_segs.push((sl, l, x));
                    }
                    new_segs.push((l, sr, ((x as i64) + md) as u64));
                } else if l <= sl && r <= sr {
                    // intersect
                    new_segs.push((sl, r, ((x as i64) + md) as u64));
                    if r != sr {
                        new_segs.push((r, sr, x));
                    }
                } else {
                    unreachable!();
                }
            }

            if md < 0 {
                treap.remove(l, r, (-md) as u64);
            } else {
                treap.add(l, r, md as u64);
            }

            segs = new_segs;
        }
    }

    #[test]
    fn check_height() {
        let mut rand = Pcg64::seed_from_u64(42);
        let mut treap = Treap::new();

        const ITS: usize = 1_000_000;
        for _ in 0..ITS {
            let mut l = rand.gen::<f64>();
            let mut r = rand.gen::<f64>();
            if l > r {
                std::mem::swap(&mut l, &mut r);
            }
            treap.add(l, r, rand.gen::<u64>() % 100);
        }

        println!("{} {}", treap.nodes(), treap.height());
        assert!(treap.nodes() <= ITS * 2 + 1); // no more than 2 new nodes per modification query
        assert!(
            treap.height() as u32 <= (ITS.ilog2() + 1) * 7,
            "height = {}, log = {}",
            treap.height(),
            ITS.ilog2()
        ); // height should be O(logn)
    }
}

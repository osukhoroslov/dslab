use std::{cell::RefCell, collections::VecDeque};

use futures::{stream::FuturesUnordered, FutureExt, StreamExt};

use dslab_core::{async_mode::UnboundedQueue, Simulation, SimulationContext};

struct Data {
    value: u32,
}

#[test]
fn test_simple_queue() {
    let mut sim = Simulation::new(123);
    let queue = sim.create_queue("queue");
    let ctx = sim.create_context("comp");

    sim.spawn(async move {
        futures::join!(
            async {
                ctx.sleep(1.).await;
                queue.put(Data { value: 1 });
                ctx.sleep(1.).await;
                queue.put(Data { value: 2 });
                ctx.sleep(1.).await;
                queue.put(Data { value: 3 });
            },
            async {
                assert_eq!(queue.take().await.value, 1);
                assert_eq!(queue.take().await.value, 2);
                assert_eq!(queue.take().await.value, 3);
            }
        );
    });

    sim.step_until_no_events();
    assert_eq!(sim.time(), 3.0);
}

#[test]
fn test_drop_receivers() {
    let mut sim = Simulation::new(123);
    let queue = sim.create_queue("queue");
    let ctx = sim.create_context("comp");

    sim.spawn(async move {
        futures::join!(
            async {
                ctx.sleep(1.).await;
                queue.put(Data { value: 1 });
                ctx.sleep(1.).await;
                queue.put(Data { value: 2 });
                ctx.sleep(1.).await;
                queue.put(Data { value: 3 });
            },
            async {
                let mut cnt_received = 0;
                futures::select! {
                    data = queue.take().fuse() => {
                        cnt_received += 1;
                        assert_eq!(data.value, 1);
                    },
                    data = queue.take().fuse() => {
                        cnt_received += 1;
                        assert_eq!(data.value, 1);
                    },
                    data = queue.take().fuse() => {
                        cnt_received += 1;
                        assert_eq!(data.value, 1);
                    }
                }
                assert_eq!(cnt_received, 1);
                let mut next = queue.take().await;
                assert_eq!(next.value, 2);
                next = queue.take().await;
                assert_eq!(next.value, 3);
                ctx.sleep(7.).await;
            }
        );
    });

    sim.step_until_no_events();
    assert_eq!(sim.time(), 10.0);
}

#[test]
fn test_drop_ready_receivers() {
    let mut sim = Simulation::new(123);
    let queue = sim.create_queue("queue");
    let ctx = sim.create_context("comp");

    sim.spawn(async move {
        futures::join!(
            async {
                ctx.sleep(10.).await;
                for data in 0..6 {
                    queue.put(Data { value: data });
                }
            },
            async {
                {
                    ctx.sleep(100.).await;
                    let mut futures = FuturesUnordered::new();
                    for _ in 0..6 {
                        futures.push(queue.take());
                    }
                    let data = futures.next().await.unwrap();
                    assert_eq!(data.value, 0);
                    assert_eq!(ctx.time(), 100.);
                }
                for expected in 1..6 {
                    let data = queue.take().await;
                    assert_eq!(data.value, expected);
                    assert_eq!(ctx.time(), 100.);
                }
                ctx.sleep(1.).await;
                let next = queue.take().await;
                unreachable!("Expected queue to be empty, but got {:?}", next.value);
            }
        );
    });

    sim.step_until_no_events();
    assert_eq!(sim.time(), 101.);
}

#[test]
fn test_drop_mixed_receivers() {
    let mut sim = Simulation::new(123);
    let queue = sim.create_queue("queue");
    let ctx = sim.create_context("comp");

    sim.spawn(async move {
        futures::join!(
            async {
                queue.put(Data { value: 1 });
                queue.put(Data { value: 2 });
                ctx.sleep(10.).await;
                queue.put(Data { value: 3 });
                queue.put(Data { value: 4 });
                queue.put(Data { value: 5 });
                ctx.sleep(1000.).await;
                queue.put(Data { value: 6 });
                ctx.sleep(10.).await;
                queue.put(Data { value: 7 });
            },
            async {
                ctx.sleep(1.).await;
                {
                    let mut futures = FuturesUnordered::new();
                    for _ in 0..4 {
                        futures.push(queue.take());
                    }
                    let data = futures.next().await.unwrap();
                    assert_eq!(ctx.time(), 1.);
                    assert_eq!(data.value, 1);
                }
                ctx.sleep(100.).await;
                {
                    let mut futures = FuturesUnordered::new();
                    for _ in 0..10 {
                        futures.push(queue.take());
                    }
                    for expected in 2..=5 {
                        let data = futures.next().await.unwrap();
                        assert_eq!(data.value, expected);
                        assert_eq!(ctx.time(), 101.);
                    }
                    let mut next = futures.next().await.unwrap();
                    assert_eq!(next.value, 6);
                    assert_eq!(ctx.time(), 1010.); // 1000 + 10 from sender
                    next = futures.next().await.unwrap();
                    assert_eq!(next.value, 7);
                    assert_eq!(ctx.time(), 1020.);
                    ctx.sleep(1.).await;
                }
            }
        );
    });

    sim.step_until_no_events();
    assert_eq!(sim.time(), 1021.0);
}

struct QueueTester {
    queue: UnboundedQueue<Data>,
    shadow_queue: RefCell<VecDeque<Data>>,
    put_counter: RefCell<u32>,
    take_counter: RefCell<u32>,
}

impl QueueTester {
    fn new(queue: UnboundedQueue<Data>) -> Self {
        Self {
            queue,
            shadow_queue: RefCell::new(VecDeque::new()),
            put_counter: RefCell::new(0),
            take_counter: RefCell::new(0),
        }
    }

    fn put(&self) {
        let counter = *self.put_counter.borrow();
        self.shadow_queue.borrow_mut().push_back(Data { value: counter });
        self.queue.put(Data { value: counter });
        *self.put_counter.borrow_mut() = counter + 1;
    }

    async fn take(&self) {
        let next = self.queue.take().await;
        assert!(
            !self.shadow_queue.borrow().is_empty(),
            "Queue is not empty, but expected to be"
        );
        let expected = self.shadow_queue.borrow_mut().pop_front().unwrap();
        assert_eq!(next.value, expected.value);
        *self.take_counter.borrow_mut() += 1;
    }

    fn assert_correct(&self) {
        let put_counter = *self.put_counter.borrow();
        let take_counter = *self.take_counter.borrow();
        let shadow_queue_len = self.shadow_queue.borrow().len();
        assert_eq!(
            put_counter,
            take_counter + shadow_queue_len as u32,
            "Put counter: {}, Take counter: {}, Shadow queue len: {}",
            put_counter,
            take_counter,
            shadow_queue_len
        );
    }
}

#[derive(Clone)]
struct Worker {
    put_prob: f64,
    create_takes: u32,
}

impl Worker {
    fn new(put_prob: f64, create_takes: u32) -> Self {
        Self { put_prob, create_takes }
    }

    async fn run(&self, ctx: &SimulationContext, queue: &QueueTester, iterations: u32) {
        for _ in 0..iterations {
            ctx.sleep(ctx.rand()).await;
            if ctx.rand() < self.put_prob {
                queue.put();
            } else {
                let mut futures = FuturesUnordered::new();
                for _ in 0..self.create_takes {
                    futures.push(queue.take());
                }
                futures.next().await.unwrap();
            }
        }
    }
}

fn stress_test(workers: Vec<Worker>, iterations: u32) {
    let mut sim = Simulation::new(123);
    let queue = sim.create_queue("queue");
    let queue_tester = QueueTester::new(queue);
    let ctx = sim.create_context("comp");

    sim.spawn(async move {
        futures::future::join_all(workers.iter().map(|w| w.run(&ctx, &queue_tester, iterations))).await;
        queue_tester.assert_correct();
    });

    sim.step_until_no_events();
}

#[test]
fn stress_test_simple() {
    stress_test(vec![Worker::new(0.5, 1); 5], 1000);
}

#[test]
fn stress_test_equal() {
    let mut workers = Vec::new();
    workers.extend(vec![Worker::new(0.5, 2); 2]);
    workers.extend(vec![Worker::new(0.2, 3); 3]);
    workers.extend(vec![Worker::new(0.8, 2); 3]);

    stress_test(workers, 1000);
}

#[test]
fn stress_test_put() {
    let mut workers = Vec::new();
    workers.extend(vec![Worker::new(1.0, 1); 2]);
    workers.extend(vec![Worker::new(0.8, 3); 3]);
    workers.extend(vec![Worker::new(0.8, 1); 3]);
    workers.extend(vec![Worker::new(0.6, 3); 3]);
    workers.extend(vec![Worker::new(0.6, 1); 3]);

    stress_test(workers, 1000);
}

#[test]
fn stress_test_take() {
    let mut workers = Vec::new();
    workers.extend(vec![Worker::new(0., 1); 2]);
    workers.extend(vec![Worker::new(0., 3); 2]);
    workers.extend(vec![Worker::new(0.2, 1); 3]);
    workers.extend(vec![Worker::new(0.2, 3); 3]);
    workers.extend(vec![Worker::new(0.4, 1); 3]);
    workers.extend(vec![Worker::new(0.4, 3); 3]);

    stress_test(workers, 1000);
}

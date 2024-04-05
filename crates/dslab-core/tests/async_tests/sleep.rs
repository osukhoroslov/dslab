use futures::{stream::FuturesUnordered, StreamExt};

use dslab_core::Simulation;

#[test]
fn test_sleep() {
    let mut sim = Simulation::new(123);
    let ctx = sim.create_context("comp");

    sim.spawn(async move {
        let sleep_time_step = 5.;
        let concurrent_sleeps = 10;

        let start_time = ctx.time();
        assert_eq!(start_time, 0.);

        ctx.sleep(sleep_time_step).await;

        assert_eq!(ctx.time(), sleep_time_step);

        let mut futures = FuturesUnordered::new();
        for i in 0..=concurrent_sleeps {
            futures.push(ctx.sleep(i as f64 * sleep_time_step));
        }

        let mut expected_next_time = sleep_time_step;
        while let Some(_) = futures.next().await {
            assert_eq!(ctx.time(), expected_next_time);
            expected_next_time += sleep_time_step;
        }

        assert_eq!(ctx.time(), ((concurrent_sleeps + 1) as f64 * sleep_time_step));
    });

    sim.step_until_no_events();
}

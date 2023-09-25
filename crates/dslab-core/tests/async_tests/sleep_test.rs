use futures::{stream::FuturesUnordered, StreamExt};

use dslab_core::Simulation;

#[test]
fn test_wait_for() {
    let mut sim = Simulation::new(42);
    let ctx = sim.create_context("dummy_worker");

    sim.spawn(async move {
        let time_wait_step = 5.;
        let concurrent_wait_items = 10;

        let start_time = ctx.time();
        assert!(start_time == 0.);

        ctx.async_sleep(time_wait_step).await;

        assert!(ctx.time() == time_wait_step);

        let mut futures = FuturesUnordered::new();
        for i in 0..=concurrent_wait_items {
            futures.push(ctx.async_sleep(i as f64 * time_wait_step));
        }

        let mut expected_next_time = time_wait_step;

        while let Some(_) = futures.next().await {
            assert!(ctx.time() == expected_next_time);
            expected_next_time += time_wait_step;
        }

        assert!(ctx.time() == ((concurrent_wait_items + 1) as f64 * time_wait_step));
    });

    sim.step_until_no_events();
}

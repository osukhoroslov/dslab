# Rust profiling using flamegraph 

Profiling programs is a very efficient way to figure out what parts of program take significant CPU time. 

In this example we present the simple possible performance improvement in DSLab Simulation. There are two ways of emitting events to the simulation: 

1. Using [`emit`](dslab_core::SimulationContext::emit) functions from [`SimulationContext`](dslab_core::SimulationContext). This way uses `BinaryHeap` to store and order incoming events.
2. Using [`emit_ordered`](dslab_core::SimulationContext::emit_ordered) functions from [`SimulationContext`](dslab_core::SimulationContext). This approach relies on user-side ordering of events and a `VecDeque` is used to store events.

In case simulation contains a component that emits already ordered events (like [`Server`](components::Server) in this example), the performance of the whole simulation may be significantly improved by using `emit_ordered` instead of `emit`.


## cargo-flamegraph 

We propose to use [`cargo flamegraph`](https://github.com/flamegraph-rs/flamegraph) profiler as it is very easy to use with Rust projects on MacOS and Linux. 

### Installation
`cargo flamegraph` can be installed with the following command: 
```bash
cargo install flamegraph
```

Full instructions see in the [`official documentation`](https://github.com/flamegraph-rs/flamegraph?tab=readme-ov-file#installation)


### Running  

The given example can be launched with the following command:
```bash
cargo flamegraph --dev --root -- --events-count 100000
```

To see how release-optimized binary performs we provide the `release-debug` profile that inherits `release` profile with optimizations but provides the debug info, which is required for profiling. The command changes as follows: 
```bash
cargo flamegraph --profile release-debug --root -- --events-count 100000
```

Both commands produce interactive `flamegraph.svg` that can be viewed using any browser. For more detailed examples, usage instructions, and the explanation of the result produced see the [`official documentation`](https://github.com/flamegraph-rs/flamegraph?tab=readme-ov-file#usage)

# Comparing results 

By launching the provided example with different arguments, you will see how [`emit_ordered`](dslab_core::SimulationContext::emit_ordered) is more efficient than [`emit`](dslab_core::SimulationContext::emit).

Here are some possible launch configurations: 
1. Debug mode with `emit`: 
    ```bash 
    cargo flamegraph --dev --root -- --events-count 5000000 --rand-clients-choose 
    ```

    The analysis of the produced `flamegraph.svg` will show that `BinaryHeap` operations take approximately 60-70% of the total time. Selection of a random client takes approximately 6% of the total time.

2. Release mode with `emit`: 
    ```bash 
     cargo flamegraph --profile release-debug --root -- --events-count 5000000 --rand-clients-choose 
    ```

    The analysis of the produced `flamegraph.svg` will show that `BinaryHeap` operations take approximately 75% of the total time. 

3. Debug mode with `emit_ordered`: 
    ```bash 
     cargo flamegraph --dev --root -- --events-count 5000000 --rand-clients-choose --use-emit-ordered
    ```

    In this case, you will see how the total time of the simulation is significantly reduced. You are expected to see 3-4 times improvement in speed. Now the majority of the time is spent on other internal `dslab-core` operations. 

4. Release mode with `emit_ordered`: 
    ```bash 
     cargo flamegraph --profile release-debug --root -- --events-count 5000000 --rand-clients-choose --use-emit-ordered
    ```

    This example shows even more significant performance improvement.

# Optimized release build 

In addition to the `release-debug` profile, we provide the `release-optimized` profile that inherits `release` profile and includes extra optimizations. The compilation time may be significantly increased, but the performance of the binary is expected to be improved by 5-10%.

Compare the following launch configurations on your machine: 
```bash
cargo run --release -- --events-count 50000000 --rand-clients-choose
cargo run --profile release-optimized -- --events-count 50000000 --rand-clients-choose
```

You can find more general tips about improving performance of Rust programs in the [`Rust Performance Book`](https://nnethercote.github.io/perf-book/) 

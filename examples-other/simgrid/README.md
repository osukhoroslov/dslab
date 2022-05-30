# SimGrid examples

## How to build

1. Create build directory

    `mkdir -p build/release; cd build/release`

2. Run CMake

    ```
    cmake                                   \
        -DSimGrid_PATH=/where/to/simgrid    \
        -DCMAKE_BUILD_TYPE=Release          \
        -DCMAKE_EXPORT_COMPILE_COMMANDS=ON  \
        ../..
    ```

    If SimGrid is installed in `/opt/simgrid` then SimGrid_PATH option can be omitted.

    To pass compile db to clangd, run 
    
    `ln -s build/release/compile_commands.json compile_commands.json`

3. Build executables with `make EXAMPLE_NAME`.


## Run examples

- [master-workers](./master-workers/README.md)

- [ping-pong](./ping-pong/README.md)


## Profiling using `perf`

1. Install [perf](https://perf.wiki.kernel.org/index.php/Main_Page) Linux profiler and [FlameGraph](https://github.com/brendangregg/FlameGraph) tools.

2. Profile execution with perf:

```
perf record --call-graph=dwarf -F 99 -g \
    bin/master-workers 1000 100000 --log=root.thres:critical
``` 

3. Prepare data for flame graph:

```
perf script | ~/tools/FlameGraph/stackcollapse-perf.pl > out.perf-folded
```

4. Build the flame graph:

```
~/tools/FlameGraph/flamegraph.pl out.perf-folded > flamegraph.svg
```


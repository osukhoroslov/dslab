# Master-workers example on SimGrid

## Build

If SimGrid is installed in `/opt/simgrid`:

```
cmake -DCMAKE_BUILD_TYPE=Release .
make
```

If SimGrid is installed somewhere else:

```
cmake -DSimGrid_PATH=/where/to/simgrid -DCMAKE_BUILD_TYPE=Release .
make
```

## Run Examples

10 hosts and 100 tasks:

```
bin/master-workers 10 100 --log=master_workers_app.thres:debug --log=no_loc
```

100 hosts and 10000 tasks:

```
bin/master-workers 100 10000 --log=root.thres:critical
```

CM02 network model:

```
bin/master-workers 100 10000 --log=root.thres:critical --cfg=network/model:CM02
```

Constant network model (requires commenting all code related to links):

```
bin/master-workers 100 10000 --log=root.thres:critical --cfg=network/model:Constant
```

## Build Flame Graph

1. Install [perf](https://perf.wiki.kernel.org/index.php/Main_Page) Linux profiler and [FlameGraph](https://github.com/brendangregg/FlameGraph) tools.

2. Profile execution with perf:

```
perf record --call-graph=dwarf -F 99 -g bin/master-workers 1000 100000 --log=root.thres:critical
``` 

3. Prepare data for flame graph:

```
perf script | ~/tools/FlameGraph/stackcollapse-perf.pl > out.perf-folded
```

4. Build the flame graph:

```
~/tools/FlameGraph/flamegraph.pl out.perf-folded > flamegraph.svg
```
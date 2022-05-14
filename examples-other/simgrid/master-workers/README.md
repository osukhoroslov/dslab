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

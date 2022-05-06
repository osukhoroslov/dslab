# Master-workers example on SimGrid

## Build

If SimGrid is installed in `/opt/simgrid`:

```
cmake .
make
```

If SimGrid is installed somewhere else:

```
cmake -DSimGrid_PATH=/where/to/simgrid .
make
```

## Run Examples

100 hosts and 10000 tasks:

```
bin/master-workers 100 10000 --log=master_workers_app.thres:error --log=no_loc
```

CM02 network model:

```
bin/master-workers 100 10000 --log=master_workers_app.thres:error --log=no_loc --cfg=network/model:CM02
```

Constant network model (requires commenting all code related to links):

```
bin/master-workers 100 10000 --log=master_workers_app.thres:error --log=no_loc --cfg=network/model:Constant
```

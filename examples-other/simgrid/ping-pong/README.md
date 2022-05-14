# Ping-pong example on SimGrid

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

2 processes with 1 peer, local, 1 iteration:

```
bin/ping-pong 2 1 0 0 1 platform.xml --log=ping_pong_app.thres:debug
```

2 processes with 1 peer, local, asymmetric mode, 1 iteration:

```
bin/ping-pong 2 1 1 0 1 platform.xml --log=ping_pong_app.thres:debug
```

2 processes with 1 peer, distributed, 1 iteration:

```
bin/ping-pong 2 1 0 1 1 platform.xml --log=ping_pong_app.thres:debug
```

2 processes with 1 peer, asymmetric mode, distributed, 1 iteration:

```
bin/ping-pong 2 1 1 1 1 platform.xml --log=ping_pong_app.thres:debug
```

1000 processes with 10 random peers, local, 100 iterations:

```
bin/ping-pong 1000 10 0 0 100 platform.xml
```

Constant network model:

```
bin/ping-pong 1000 10 0 0 1000 platform-constant.xml --log=root.thres:critical --cfg=network/model:Constant
```
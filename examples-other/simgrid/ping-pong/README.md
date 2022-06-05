# Ping-pong example on SimGrid

## Run Examples

2 processes with 1 peer, local, 1 iteration:

```
bin/ping-pong 2 1 0 0 1 ../../ping-pong/platform.xml --log=ping_pong_app.thres:info
```

2 processes with 1 peer, local, asymmetric mode, 1 iteration:

```
bin/ping-pong 2 1 1 0 1 ../../ping-pong/platform.xml --log=ping_pong_app.thres:info
```

2 processes with 1 peer, distributed, 1 iteration:

```
bin/ping-pong 2 1 0 1 1 ../../ping-pong/platform.xml --log=ping_pong_app.thres:info
```

2 processes with 1 peer, asymmetric mode, distributed, 1 iteration:

```
bin/ping-pong 2 1 1 1 1 ../../ping-pong/platform.xml --log=ping_pong_app.thres:info
```

1000 processes with 10 random peers, local, 100 iterations:

```
bin/ping-pong 1000 10 0 0 100 ../../ping-pong/platform.xml
```

Constant network model (all communications take 1 time unit):

```
bin/ping-pong 1000 10 0 0 1000 ../../ping-pong/platform-constant.xml --log=root.thres:critical --cfg=network/model:Constant
```

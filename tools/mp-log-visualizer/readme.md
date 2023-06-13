# 💫 dslab-mp log visualizer 💫

<video src="https://github.com/amgfrthsp/dslab/assets/80028987/0b8c1981-ac2b-4c9f-aa3f-6dc0be871629" controls="controls">
</video>

This visualizer can help you with your Distributed Systems homework. It's a tool which visualizes algorithms you've implemented. 

Have a look at how it works by visualizing some examples. From `tools/mp-log-visualizer` run

```
cargo run examples/ping-pong.txt
```

## How to run 
In order to visualize a specific test, go to the homework directory and run 
```
cargo run -t "TEST_NAME" -v 
```
If you want the state of your nodes to be visible in animation, wrap their fields in `StateMember` struct. Here is an example from the `guarantees` homework: 

```
from dslabmp import Context, Message, Process, StateMember

class AtMostOnceSender(Process):
    def __init__(self, node_id: str, receiver_id: str):
        self._id = StateMember(node_id)
        self._receiver = StateMember(receiver_id)
```
## How to use 
### Keyboard and mouse 
- `Space` - Stop / Continue animation
- `Down / Up` - Slow down / Speed up animation
- `Right` - Go to next event in the system. It can be helpful when, for example, there is a big gap between two events and you don't want to wait for long. You can see time of next event at the top of Config window. 
- `- / +` - Decrease / Increase scale
- Right click on a node or a message to see related information
- Hover over a timer to see related information


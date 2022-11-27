use std::env;

use crate::PyProcessFactory;
use dslab_mp::{message::Message, process::Process, system::System};

fn build_system() -> (System, String) {
    let mut sys = System::new(0);
    sys.add_node("node");
    let node_f = PyProcessFactory::new("python-tests/node.py", "Node");
    let node = node_f.build(("node",), 1);
    let serialized = node.serialize();
    sys.add_process("node", Box::new(node), "node");
    return (sys, serialized);
}

#[test]
fn test() {
    env::set_var("PYTHONPATH", "python");
    let (mut sys, serialized) = build_system();
    let data = r#"{"value": "Hello!"}"#;
    sys.send_local_message("node", Message::new("echo", data));
    sys.step_until_no_events();

    // node sends local message only if it has a secret which is not a state member
    let msgs = sys.read_local_messages("node");
    assert_eq!(msgs.len(), 1);

    // we consider node not having anything but state members after deserialize
    sys.get_node("node")
        .borrow()
        .get_process("node")
        .deserialize(&serialized);
    sys.send_local_message("node", Message::new("echo", data));
    sys.step_until_no_events();
    let msgs = sys.read_local_messages("node");
    assert_eq!(msgs.len(), 0);
}

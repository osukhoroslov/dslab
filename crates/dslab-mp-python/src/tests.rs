use std::env;

use crate::PyProcessFactory;
use dslab_mp::{message::Message, process::Process, system::System};

fn build_system() -> (System, String) {
    let mut sys = System::new(0);
    sys.add_node("node");
    let proc_f = PyProcessFactory::new("python-tests/process.py", "TestProcess");
    let process = proc_f.build(("node",), 1);
    let state = process.state();
    sys.add_process("proc", Box::new(process), "node");
    return (sys, state);
}

#[test]
fn test_set_state() {
    env::set_var("PYTHONPATH", "python");
    let (mut sys, proc_state) = build_system();
    let data = r#"{"value": "Hello!"}"#;
    sys.send_local_message("proc", Message::new("FIRST_STEP", data));
    sys.step_until_no_events();

    // process should not have anything but state members after `set_state()`
    sys.get_node("node")
        .unwrap()
        .borrow()
        .get_process("proc")
        .unwrap()
        .set_state(&proc_state);
    sys.send_local_message("proc", Message::new("SECOND_STEP", data));
    sys.step_until_no_events();
}

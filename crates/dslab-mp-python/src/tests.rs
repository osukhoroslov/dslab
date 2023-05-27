use std::env;

use crate::PyProcessFactory;
use dslab_mp::process::ProcessState;
use dslab_mp::{message::Message, process::Process, system::System};

fn build_system() -> (System, Box<dyn ProcessState>) {
    let mut sys = System::new(0);
    sys.add_node("node");
    let proc_f = PyProcessFactory::new("python-tests/process.py", "TestProcess");
    let process = proc_f.build(("node",), 1);
    let state = process.state();
    sys.add_process("proc", Box::new(process), "node");
    (sys, state)
}

#[test]
fn test_set_state() {
    env::set_var("PYTHONPATH", "python");
    let (mut sys, proc_state) = build_system();
    let data = r#"{"value": "Hello!"}"#;
    sys.send_local_message("proc", Message::new("echo", data));
    sys.step_until_no_events();

    // process sends local message only if it has a secret which is not a state member
    let msgs = sys.read_local_messages("proc");
    assert_eq!(msgs.len(), 1);

    // process should not have anything but state members after `set_state()`
    sys.get_mut_node("node").unwrap().set_process_state("proc", proc_state);
    sys.send_local_message("proc", Message::new("echo", data));
    sys.step_until_no_events();
    let msgs = sys.read_local_messages("proc");
    assert_eq!(msgs.len(), 0);
}

use std::env;
use std::rc::Rc;

use serde::Serialize;

use crate::PyProcessFactory;
use dslab_mp::process::ProcessState;
use dslab_mp::{message::Message, process::Process, system::System};

fn build_system() -> (System, Rc<dyn ProcessState>) {
    let mut sys = System::new(0);
    sys.add_node("node");
    let proc_f = PyProcessFactory::new("python-tests/process.py", "TestProcess");
    let process = proc_f.build((), 1);
    let state = process.state().unwrap();
    sys.add_process("proc", Box::new(process), "node");
    (sys, state)
}

#[derive(Serialize)]
struct EmptyMessage {}

#[test]
fn test_set_state() {
    env::set_var("PYTHONPATH", "python");
    let (mut sys, proc_state) = build_system();

    // check that process stores valid data
    // and update inner state (with `tmp_value` member)
    sys.send_local_message("proc", Message::json("CHECK_STATE", &EmptyMessage {}));
    sys.step_until_no_events();

    // reset process state to initial
    sys.get_mut_node("node").unwrap().set_process_state("proc", proc_state);
    // check that process stores the same data as right after initialization
    sys.send_local_message("proc", Message::json("CHECK_STATE", &EmptyMessage {}));
    sys.step_until_no_events();
}

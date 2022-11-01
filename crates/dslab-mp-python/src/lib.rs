use std::fs;
use std::rc::Rc;

use pyo3::prelude::*;
use pyo3::types::{PyModule, PyTuple};

use dslab_mp::context::Context;
use dslab_mp::message::Message;
use dslab_mp::process::Process;

pub struct PyProcessFactory {
    proc_class: PyObject,
    msg_class: Rc<PyObject>,
    ctx_class: Rc<PyObject>,
    get_size_fun: Rc<Py<PyAny>>,
}

impl PyProcessFactory {
    pub fn new(impl_path: &str, impl_class: &str) -> Self {
        let impl_code = fs::read_to_string(impl_path).unwrap();
        let impl_realpath = fs::canonicalize(impl_path).unwrap();
        let impl_filename = impl_realpath.to_str().unwrap();
        let impl_module = impl_filename.replace(".py", "");
        let classes = Python::with_gil(|py| -> (PyObject, PyObject, PyObject, Py<PyAny>) {
            let impl_module = PyModule::from_code(py, impl_code.as_str(), impl_filename, &impl_module).unwrap();
            let proc_class = impl_module.getattr(impl_class).unwrap().to_object(py);
            let msg_class = impl_module.getattr("Message").unwrap().to_object(py);
            let ctx_class = impl_module.getattr("Context").unwrap().to_object(py);
            let get_size_fun = get_size_fun(py);
            (proc_class, msg_class, ctx_class, get_size_fun)
        });
        Self {
            proc_class: classes.0,
            msg_class: Rc::new(classes.1),
            ctx_class: Rc::new(classes.2),
            get_size_fun: Rc::new(classes.3),
        }
    }

    pub fn build(&self, args: impl IntoPy<Py<PyTuple>>, seed: u64) -> PyProcess {
        let proc = Python::with_gil(|py| -> PyObject {
            py.run(format!("import random\nrandom.seed({})", seed).as_str(), None, None)
                .unwrap();
            self.proc_class
                .call1(py, args)
                .map_err(|e| log_python_error(e, py))
                .unwrap()
                .to_object(py)
        });
        PyProcess {
            proc,
            msg_class: self.msg_class.clone(),
            ctx_class: self.ctx_class.clone(),
            get_size_fun: self.get_size_fun.clone(),
            max_size: 0,
            max_size_freq: 0,
            max_size_counter: 0,
        }
    }
}

pub struct PyProcess {
    proc: PyObject,
    msg_class: Rc<PyObject>,
    ctx_class: Rc<PyObject>,
    get_size_fun: Rc<Py<PyAny>>,
    max_size: u64,
    max_size_freq: u32,
    max_size_counter: u32,
}

impl PyProcess {
    pub fn set_max_size_freq(&mut self, freq: u32) {
        self.max_size_freq = freq;
        self.max_size_counter = 1;
    }

    fn handle_proc_actions(ctx: &mut Context, py_ctx: &PyObject, py: Python) {
        let sent: Vec<(String, String, String)> = py_ctx.getattr(py, "_sent_messages").unwrap().extract(py).unwrap();
        for m in sent {
            ctx.send(Message::new(&m.0, &m.1), m.2);
        }
        let sent_local: Vec<(String, String)> =
            py_ctx.getattr(py, "_sent_local_messages").unwrap().extract(py).unwrap();
        for m in sent_local {
            ctx.send_local(Message::new(&m.0, &m.1));
        }
        let timer_actions: Vec<(String, f64)> = py_ctx.getattr(py, "_timer_actions").unwrap().extract(py).unwrap();
        for t in timer_actions {
            if t.1 < 0.0 {
                ctx.cancel_timer(&t.0);
            } else {
                ctx.set_timer(&t.0, t.1);
            }
        }
    }

    fn update_max_size(&mut self, py: Python, force_update: bool) {
        if self.max_size_freq > 0 {
            self.max_size_counter -= 1;
            if self.max_size_counter == 0 || force_update {
                let size: u64 = self.get_size_fun.call1(py, (&self.proc,)).unwrap().extract(py).unwrap();
                self.max_size = self.max_size.max(size);
                self.max_size_counter = self.max_size_freq;
            }
        }
    }
}

impl Process for PyProcess {
    fn on_message(&mut self, msg: Message, from: String, ctx: &mut Context) {
        Python::with_gil(|py| {
            let py_msg = self
                .msg_class
                .call_method1(py, "from_json", (msg.tip, msg.data))
                .unwrap();
            let py_ctx = self.ctx_class.call1(py, (ctx.time(),)).unwrap();
            self.proc
                .call_method1(py, "on_message", (py_msg, from, &py_ctx))
                .map_err(|e| log_python_error(e, py))
                .unwrap();
            PyProcess::handle_proc_actions(ctx, &py_ctx, py);
            self.update_max_size(py, false);
        });
    }

    fn on_local_message(&mut self, msg: Message, ctx: &mut Context) {
        Python::with_gil(|py| {
            let py_msg = self
                .msg_class
                .call_method1(py, "from_json", (msg.tip, msg.data))
                .unwrap();
            let py_ctx = self.ctx_class.call1(py, (ctx.time(),)).unwrap();
            self.proc
                .call_method1(py, "on_local_message", (py_msg, &py_ctx))
                .map_err(|e| log_python_error(e, py))
                .unwrap();
            PyProcess::handle_proc_actions(ctx, &py_ctx, py);
            self.update_max_size(py, false);
        });
    }

    fn on_timer(&mut self, timer: String, ctx: &mut Context) {
        Python::with_gil(|py| {
            let py_ctx = self.ctx_class.call1(py, (ctx.time(),)).unwrap();
            self.proc
                .call_method1(py, "on_timer", (timer, &py_ctx))
                .map_err(|e| log_python_error(e, py))
                .unwrap();
            PyProcess::handle_proc_actions(ctx, &py_ctx, py);
            self.update_max_size(py, false);
        });
    }

    fn max_size(&mut self) -> u64 {
        Python::with_gil(|py| self.update_max_size(py, true));
        self.max_size
    }
}

fn log_python_error(e: PyErr, py: Python) -> PyErr {
    eprintln!("\n!!! Error when calling Python code:\n");
    e.print(py);
    eprintln!();
    e
}

fn get_size_fun(py: Python) -> Py<PyAny> {
    PyModule::from_code(
        py,
        "
import sys

def get_size(obj, seen=None):
    size = sys.getsizeof(obj)
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    seen.add(obj_id)
    if isinstance(obj, dict):
        size += sum([get_size(v, seen) for v in obj.values()])
        size += sum([get_size(k, seen) for k in obj.keys()])
    elif hasattr(obj, '__dict__'):
        size += get_size(obj.__dict__, seen)
    elif hasattr(obj, '__slots__'):
        size += sum([get_size(getattr(obj, slot), seen) for slot in obj.__slots__])
    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
        size += sum([get_size(i, seen) for i in obj])
    return size",
        "",
        "",
    )
    .unwrap()
    .getattr("get_size")
    .unwrap()
    .into()
}

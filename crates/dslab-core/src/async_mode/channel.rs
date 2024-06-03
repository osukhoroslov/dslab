use std::{cell::RefCell, collections::VecDeque, rc::Rc};

#[derive(Clone)]
pub(crate) struct Receiver<T> {
    data: Rc<RefCell<VecDeque<T>>>,
}

impl<T> Receiver<T> {
    pub fn new(data: Rc<RefCell<VecDeque<T>>>) -> Self {
        Self { data }
    }

    pub fn try_recv(&self) -> Option<T> {
        self.data.borrow_mut().pop_front()
    }
}

#[derive(Clone)]
pub(crate) struct Sender<T> {
    data: Rc<RefCell<VecDeque<T>>>,
}

impl<T> Sender<T> {
    pub fn new(data: Rc<RefCell<VecDeque<T>>>) -> Self {
        Self { data }
    }

    pub fn send(&self, value: T) {
        self.data.borrow_mut().push_back(value);
    }
}

pub(crate) fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let data = Rc::new(RefCell::new(VecDeque::new()));
    (Sender::new(data.clone()), Receiver::new(data))
}

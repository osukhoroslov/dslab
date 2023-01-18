use std::cell::RefCell;
use std::rc::Rc;

use crate::mc::system::McSystem;

pub trait Strategy {
    fn run(&mut self, system: Rc<RefCell<McSystem>>) -> bool;
}

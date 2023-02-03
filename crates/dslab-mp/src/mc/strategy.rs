use crate::mc::system::McSystem;

pub trait Strategy {
    fn run(&mut self, system: &mut McSystem) -> bool;
}

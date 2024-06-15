use crate::async_mode::run_async_example;
use crate::callbacks::run_callbacks_example;

mod async_mode;
mod callbacks;

fn main() {
    run_callbacks_example();
    run_async_example();
}

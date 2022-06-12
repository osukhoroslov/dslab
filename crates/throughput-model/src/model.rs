pub trait ThroughputModel<T> {
    fn insert(&mut self, current_time: f64, volume: f64, item: T);
    fn pop(&mut self) -> Option<(f64, T)>;
    fn peek(&self) -> Option<(f64, &T)>;
}

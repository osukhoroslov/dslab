use simcore::component::Fractional;

pub trait ThroughputModel<T> {
    fn insert(&mut self, current_time: Fractional, volume: Fractional, item: T);
    fn pop(&mut self) -> Option<(Fractional, T)>;
    fn next_time(&self) -> Option<(Fractional, &T)>;
}

/// Runs model checking with given settings
#[macro_export]
macro_rules! run_mc {
    ($sys:expr, $config:expr, $strategy:ident) => {
        match $strategy {
            "bfs" => ModelChecker::new(&$sys).run::<Bfs>($config),
            "dfs" => ModelChecker::new(&$sys).run::<Dfs>($config),
            s => panic!("Unknown strategy name: {}", s),
        }
    };
    ($sys:expr, $config:expr, $strategy:ident, $callback:expr) => {
        match $strategy {
            "bfs" => ModelChecker::new(&$sys).run_with_change::<Bfs>($config, $callback),
            "dfs" => ModelChecker::new(&$sys).run_with_change::<Dfs>($config, $callback),
            s => panic!("Unknown strategy name: {}", s),
        }
    };
    ($sys:expr, $config:expr, $strategy:ident, $states:ident, $callback:expr) => {
        match $strategy {
            "bfs" => ModelChecker::new(&$sys).run_from_states_with_change::<Bfs>($config, $states, $callback),
            "dfs" => ModelChecker::new(&$sys).run_from_states_with_change::<Dfs>($config, $states, $callback),
            s => panic!("Unknown strategy name: {}", s),
        }
    };
}

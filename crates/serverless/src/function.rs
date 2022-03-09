use crate::util::Counter;

use std::collections::HashMap;

pub struct Function {
    container_deployment_time: f64,
}

impl Function {
    pub fn new(container_deployment_time: f64) -> Self {
        Self {
            container_deployment_time,
        }
    }

    pub fn get_deployment_time(&self) -> f64 {
        self.container_deployment_time
    }
}

#[derive(Default)]
pub struct FunctionManager {
    function_ctr: Counter,
    functions: HashMap<u64, Function>,
}

impl FunctionManager {
    pub fn get_function(&self, id: u64) -> Option<&Function> {
        self.functions.get(&id)
    }

    pub fn new_function(&mut self, f: Function) -> u64 {
        let id = self.function_ctr.next();
        self.functions.insert(id, f);
        id
    }
}

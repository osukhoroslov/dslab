use crate::resource::ResourceConsumer;
use crate::util::Counter;

use std::collections::HashMap;

pub struct Function {
    container_deployment_time: f64,
    container_resources: ResourceConsumer,
}

impl Function {
    pub fn new(container_deployment_time: f64, container_resources: ResourceConsumer) -> Self {
        Self {
            container_deployment_time,
            container_resources,
        }
    }

    pub fn get_deployment_time(&self) -> f64 {
        self.container_deployment_time
    }

    pub fn get_resources(&self) -> &ResourceConsumer {
        &self.container_resources
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

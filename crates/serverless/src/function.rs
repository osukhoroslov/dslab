use crate::resource::ResourceConsumer;
use crate::util::Counter;

use std::collections::HashMap;

// A group shares a common container image.
// Functions from the same group can be executed
// on the same container (sequentially).
pub struct Group {
    container_deployment_time: f64,
    container_resources: ResourceConsumer,
}

impl Group {
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

pub struct Function {
    pub group_id: u64,
}

impl Function {
    pub fn new(group_id: u64) -> Self {
        Self { group_id }
    }
}

#[derive(Default)]
pub struct FunctionManager {
    function_ctr: Counter,
    functions: HashMap<u64, Function>,
    group_ctr: Counter,
    groups: HashMap<u64, Group>,
}

impl FunctionManager {
    pub fn get_function(&self, id: u64) -> Option<&Function> {
        self.functions.get(&id)
    }

    pub fn get_group(&self, id: u64) -> Option<&Group> {
        self.groups.get(&id)
    }

    pub fn new_function(&mut self, f: Function) -> u64 {
        let id = self.function_ctr.next();
        self.functions.insert(id, f);
        id
    }

    pub fn new_function_with_group(&mut self, g: Group) -> u64 {
        let group_id = self.new_group(g);
        self.new_function(Function::new(group_id))
    }

    pub fn new_group(&mut self, g: Group) -> u64 {
        let id = self.group_ctr.next();
        self.groups.insert(id, g);
        id
    }
}

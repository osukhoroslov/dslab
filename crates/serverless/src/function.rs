use std::collections::HashMap;

use crate::resource::ResourceConsumer;
use crate::util::Counter;

// An application shares a common container image.
// Functions from the same application can be executed
// on the same container (limited by concurrent_invocations
// field).
pub struct Application {
    pub id: u64,
    concurrent_invocations: usize,
    container_deployment_time: f64,
    container_resources: ResourceConsumer,
}

impl Application {
    pub fn new(
        concurrent_invocations: usize,
        container_deployment_time: f64,
        container_resources: ResourceConsumer,
    ) -> Self {
        Self {
            id: u64::MAX,
            concurrent_invocations,
            container_deployment_time,
            container_resources,
        }
    }

    pub fn get_concurrent_invocations(&self) -> usize {
        self.concurrent_invocations
    }

    pub fn get_deployment_time(&self) -> f64 {
        self.container_deployment_time
    }

    pub fn get_resources(&self) -> &ResourceConsumer {
        &self.container_resources
    }
}

pub struct Function {
    pub app_id: u64,
}

impl Function {
    pub fn new(app_id: u64) -> Self {
        Self { app_id }
    }
}

#[derive(Default)]
pub struct FunctionRegistry {
    app_ctr: Counter,
    apps: HashMap<u64, Application>,
    function_ctr: Counter,
    functions: HashMap<u64, Function>,
}

impl FunctionRegistry {
    pub fn get_function(&self, id: u64) -> Option<&Function> {
        self.functions.get(&id)
    }

    pub fn get_app(&self, id: u64) -> Option<&Application> {
        self.apps.get(&id)
    }

    pub fn add_function(&mut self, f: Function) -> u64 {
        let id = self.function_ctr.next();
        self.functions.insert(id, f);
        id
    }

    pub fn add_app_with_single_function(&mut self, a: Application) -> u64 {
        let app_id = self.add_app(a);
        self.add_function(Function::new(app_id))
    }

    pub fn add_app(&mut self, mut a: Application) -> u64 {
        let id = self.app_ctr.next();
        a.id = id;
        self.apps.insert(id, a);
        id
    }
}

use crate::resource::ResourceConsumer;

/// An application shares a common container image.
/// Functions from the same application can be executed on the same container (limited by concurrent_invocations field).
pub struct Application {
    pub id: usize,
    concurrent_invocations: usize,
    container_deployment_time: f64,
    container_cpu_share: f64,
    container_resources: ResourceConsumer,
}

impl Application {
    pub fn new(
        concurrent_invocations: usize,
        container_deployment_time: f64,
        container_cpu_share: f64,
        container_resources: ResourceConsumer,
    ) -> Self {
        Self {
            id: usize::MAX,
            concurrent_invocations,
            container_deployment_time,
            container_cpu_share,
            container_resources,
        }
    }

    pub fn get_concurrent_invocations(&self) -> usize {
        self.concurrent_invocations
    }

    pub fn get_deployment_time(&self) -> f64 {
        self.container_deployment_time
    }

    pub fn get_cpu_share(&self) -> f64 {
        self.container_cpu_share
    }

    pub fn get_resources(&self) -> &ResourceConsumer {
        &self.container_resources
    }
}

pub struct Function {
    pub app_id: usize,
}

impl Function {
    pub fn new(app_id: usize) -> Self {
        Self { app_id }
    }
}

#[derive(Default)]
pub struct FunctionRegistry {
    apps: Vec<Application>,
    functions: Vec<Function>,
}

impl FunctionRegistry {
    pub fn get_function(&self, id: usize) -> Option<&Function> {
        if id < self.functions.len() {
            Some(&self.functions[id])
        } else {
            None
        }
    }

    pub fn get_app(&self, id: usize) -> Option<&Application> {
        if id < self.apps.len() {
            Some(&self.apps[id])
        } else {
            None
        }
    }

    pub fn get_app_by_function(&self, id: usize) -> Option<&Application> {
        if let Some(func) = self.get_function(id) {
            self.get_app(func.app_id)
        } else {
            None
        }
    }

    pub fn add_function(&mut self, f: Function) -> usize {
        let id = self.functions.len();
        self.functions.push(f);
        id
    }

    pub fn add_app_with_single_function(&mut self, a: Application) -> usize {
        let app_id = self.add_app(a);
        self.add_function(Function::new(app_id))
    }

    pub fn add_app(&mut self, mut a: Application) -> usize {
        let id = self.apps.len();
        a.id = id;
        self.apps.push(a);
        id
    }
}

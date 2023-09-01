//! Function and application models.
use crate::resource::ResourceConsumer;

/// An application shares a common container image.
/// Functions from the same application can be executed on the same container (limited by `concurrent_invocations` field).
pub struct Application {
    /// Application id.
    pub id: usize,
    concurrent_invocations: usize,
    container_deployment_time: f64,
    container_cpu_share: f64,
    container_resources: ResourceConsumer,
}

impl Application {
    /// Creates new application.
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

    /// Returns maximum possible number of invocations that can be run simultaneously on one container of this application.
    pub fn get_concurrent_invocations(&self) -> usize {
        self.concurrent_invocations
    }

    /// Returns the time needed to deploy one container of this application.
    pub fn get_deployment_time(&self) -> f64 {
        self.container_deployment_time
    }

    /// Returns CPU share required by containers of this application.
    pub fn get_cpu_share(&self) -> f64 {
        self.container_cpu_share
    }

    /// Returns resources required by containers of this application.
    pub fn get_resources(&self) -> &ResourceConsumer {
        &self.container_resources
    }
}

/// A single function of an application.
pub struct Function {
    /// Application id.
    pub app_id: usize,
}

impl Function {
    /// Creates new function.
    pub fn new(app_id: usize) -> Self {
        Self { app_id }
    }
}

/// Stores information about apps and functions.
#[derive(Default)]
pub struct FunctionRegistry {
    apps: Vec<Application>,
    functions: Vec<Function>,
}

impl FunctionRegistry {
    /// Returns a reference to a [`Function`] by its `id`.
    pub fn get_function(&self, id: usize) -> Option<&Function> {
        if id < self.functions.len() {
            Some(&self.functions[id])
        } else {
            None
        }
    }

    /// Returns a reference to an [`Application`] by its `id`.
    pub fn get_app(&self, id: usize) -> Option<&Application> {
        if id < self.apps.len() {
            Some(&self.apps[id])
        } else {
            None
        }
    }

    /// Returns a reference to an [`Application`] that owns a [`Function`] specified by `id`.
    pub fn get_app_by_function(&self, id: usize) -> Option<&Application> {
        if let Some(func) = self.get_function(id) {
            self.get_app(func.app_id)
        } else {
            None
        }
    }

    /// Adds a new [`Function`] and returns its `id`.
    pub fn add_function(&mut self, f: Function) -> usize {
        let id = self.functions.len();
        self.functions.push(f);
        id
    }

    /// Adds a new [`Application`] consisting of a single [`Function`] and returns its `id`.
    pub fn add_app_with_single_function(&mut self, a: Application) -> usize {
        let app_id = self.add_app(a);
        self.add_function(Function::new(app_id))
    }

    /// Adds a new [`Application`] and returns its `id`. Note: the application is created without any functions, they must be added separately.
    pub fn add_app(&mut self, mut a: Application) -> usize {
        let id = self.apps.len();
        a.id = id;
        self.apps.push(a);
        id
    }
}

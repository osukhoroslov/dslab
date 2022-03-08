use core::simulation::Simulation;

use serverless::function::Function;
use serverless::invoker::InvocationRequest;
use serverless::simulation::ServerlessSimulation;

fn main() {
    let sim = Simulation::new(1);
    let mut serverless = ServerlessSimulation::new(sim);
    for i in 0..2 {
        serverless.new_host();
    }
    let fast = serverless.new_function(Function::new(1.));
    let slow = serverless.new_function(Function::new(2.));
    serverless.send_invocation_request(0., InvocationRequest { id: fast, duration: 1.0 });
    serverless.send_invocation_request(0., InvocationRequest { id: fast, duration: 1.0 });
    serverless.send_invocation_request(0., InvocationRequest { id: slow, duration: 1.0 });
}

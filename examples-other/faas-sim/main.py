import argparse
import itertools
import logging
import numpy as np
import pandas as pd
import random
import time

from collections import deque
from pathlib import Path
from typing import List

import ether.scenarios.urbansensing as scenario
from ether.blocks.cells import FiberToExchange
from ether.blocks.nodes import create_node
from ether.cell import LANCell
from skippy.core.utils import parse_size_string

from sim import docker
from sim.benchmark import Benchmark
from sim.core import Environment
from sim.docker import ImageProperties
from sim.faas import FunctionDeployment, Function, FunctionImage, ResourceConfiguration, ScalingConfiguration, \
    FunctionContainer, SimulatorFactory, FunctionSimulator, FunctionRequest, FunctionState, DefaultFaasSystem, LoadBalancer
from sim.faas.system import simulate_function_invocation, simulate_function_start
from sim.faassim import Simulation
from sim.topology import Topology

logger = logging.getLogger(__name__)

COLDSTART_TIME = 0.5


def create_host():
    if not hasattr(create_host, "counter"):
        create_host.counter = 0
    idx = create_host.counter
    create_host.counter += 1
    return create_node(name=f'host_{idx}', cpus=4, mem='4096M', arch='x86', labels={ 
                           'ether.edgerun.io/type': 'host',
                           'ether.edgerun.io/model': 'host'})


def create_topology() -> Topology:
    t = Topology()
    cloud = LANCell([create_host] * 100, FiberToExchange('internet'))
    cloud.materialize(t)
    t.init_docker_registry()
    return t


def parse_arrival_profile(df, idx, queue):
    points = []
    data = df[df.Invocations > 0].values
    times = data[:, 0] / 1000
    durs = data[:, 2] / 1000
    cnts = data[:, 1]
    for i in range(len(cnts)):
        cnt = cnts[i]
        t = times[i]
        dur = durs[i]
        for _ in range(cnt):
            points.append((t / 1000, idx))
            queue.append(dur / 1000)
    return points


def parse_all(data, queues):
    l = []
    for idx, (name, df) in enumerate(data.items()):
        l.append(parse_arrival_profile(df, idx, queues[name]))
    total = sorted(list(itertools.chain.from_iterable(l)))
    return total


def invoke_all(env, deployments, invocations):
    old = 0
    for t, idx in invocations:
        yield env.timeout(t - old)
        env.process(env.faas.invoke(FunctionRequest(deployments[idx].name)))
        old = t
    yield env.timeout(5)


def main(data_dir):
    random.seed(1)
    logging.basicConfig(level=logging.CRITICAL)

    topology = create_topology()

    # a benchmark is a simpy process that sets up the runtime system (e.g., creates container images, deploys functions)
    # and creates workload by simulating function requests
    benchmark = ExampleBenchmark(data_dir)
    queues = benchmark.populate()
    print('Trace reading finished!')

    # a simulation runs until the benchmark process terminates
    sim = Simulation(topology, benchmark)
    sim.create_simulator_factory = lambda: CustomSimulatorFactory(queues)
    sim.create_faas_system = CustomFaasSystem
    begin = time.time()
    sim.run()
    end = time.time()
    print('simulation time = {:.3f}'.format(end - begin))


class CustomSimulatorFactory(SimulatorFactory):
    def __init__(self, queues):
        self.queues = queues

    def create(self, env: Environment, container: FunctionContainer) -> FunctionSimulator:
        return CustomFunctionSimulator(container.resource_config.get_resource_requirements()['memory'], self.queues[container.fn_image.image])


class CustomFunctionSimulator(FunctionSimulator):
    def __init__(self, mem, q):
        self.mem = mem
        self.queue = q

    def deploy(self, env, replica):
        yield env.timeout(0)

    def startup(self, env, replica):
        yield env.timeout(COLDSTART_TIME)

    def setup(self, env, replica):
        yield env.timeout(0)

    def invoke(self, env, replica, request):
        replica.node.current_requests.add(request)
        
        replica.state = FunctionState.SUSPENDED
        duration = self.queue.popleft()
        yield env.timeout(duration)

        replica.state = FunctionState.RUNNING

        replica.node.current_requests.remove(request)

    def teardown(self, env, replica):
        yield env.timeout(0)


class FixedResourceConfiguration(ResourceConfiguration):
    def __init__(self, memory):
        self.memory = memory

    def get_resource_requirements(self):
        return {'cpu': 0, 'memory': self.memory}


class ExampleBenchmark(Benchmark):
    def __init__(self, data_dir):
        self.__data = {}
        for item in data_dir.glob('*.csv'):
            name = item.stem
            self.__data[name] = pd.read_csv(item).rename(columns=lambda x: x.strip())

    def populate(self):
        queues = {name: deque() for name in self.__data.keys()}
        self.__invocations = parse_all(self.__data, queues)
        return queues

    def setup(self, env: Environment):
        containers: docker.ContainerRegistry = env.container_registry

        # populate the global container registry with images
        for name, data in self.__data.items():
            containers.put(ImageProperties(name, parse_size_string('0M'), arch='x86'))

        # log all the images in the container
        for name, tag_dict in containers.images.items():
            for tag, images in tag_dict.items():
                logger.info('%s, %s, %s', name, tag, images)

    def run(self, env: Environment):
        # deploy functions
        deployments = self.prepare_deployments()

        for deployment in deployments:
            yield from env.faas.deploy(deployment)

        yield from invoke_all(env, deployments, self.__invocations)

    def prepare_deployments(self) -> List[FunctionDeployment]:
        return [self.prepare_function_deployment(name) for name in self.__data.keys()]

    def prepare_function_deployment(self, name):
        image = FunctionImage(image=name)
        fn = Function(name, fn_images=[image])

        mem = self.__data[name]['Provisioned Memory [mb]'].max()
        fn_container = FunctionContainer(image, FixedResourceConfiguration(parse_size_string(str(mem) + 'M')))

        scaling = ScalingConfiguration()
        scaling.scale_min = 0
        scaling.scale_max = 1000
        scaling.scale_zero = True
        return FunctionDeployment(
            fn,
            [fn_container],
            scaling
        )


class CustomFaasSystem(DefaultFaasSystem):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.upscale_queue = {}
        self.downscale_in_progress = {}

    def invoke(self, request):
        logger.debug('invoking function %s', request.name)
        if request.name not in self.upscale_queue:
            q = deque()
            self.upscale_queue[request.name] = q
            self.downscale_in_progress[request.name] = False

        if request.name not in self.functions_deployments.keys():
            logger.warning('invoking non-existing function %s', request.name)
            return

        yield from self.clear_stale_replicas(request.name)
        t_received = self.env.now

        replicas = self.get_replicas(request.name, FunctionState.RUNNING)
        while not replicas:
            '''
            https://docs.openfaas.com/architecture/autoscaling/#scaling-up-from-zero-replicas
            When scale_from_zero is enabled a cache is maintained in memory indicating the readiness of each function.
            If when a request is received a function is not ready, then the HTTP connection is blocked, the function is
            scaled to min replicas, and as soon as a replica is available the request is proxied through as per normal.
            You will see this process taking place in the logs of the gateway component.
            '''
            self.upscale_queue[request.name].append(self.env.now)
            yield from self.make_new_replica(request.name)
            yield self.env.timeout(COLDSTART_TIME)
            replicas = self.get_replicas(request.name, FunctionState.RUNNING)

        if len(replicas) < 1:
            raise ValueError
        elif len(replicas) > 1:
            logger.debug('asking load balancer for replica for request %s:%d', request.name, request.request_id)
            replica = self.next_replica(request)
        else:
            replica = random.choice(replicas)

        logger.debug('dispatching request %s:%d to %s', request.name, request.request_id, replica.node.name)

        t_start = self.env.now
        yield from simulate_function_invocation(self.env, replica, request)

        t_end = self.env.now

        t_wait = t_start - t_received
        t_exec = t_end - t_start
        self.env.metrics.log_invocation(request.name, replica.image, replica.node.name, t_wait, t_start,
                                        t_exec, id(replica))

    def make_new_replica(self, name):
        yield from self.scale_up(name, 1)

    def clear_stale_replicas(self, name):
        if self.downscale_in_progress[name]:
            return
        self.downscale_in_progress[name] = True
        q = self.upscale_queue[name]
        while len(q) > 0 and q[0] < self.env.now - 600 and len(self.get_replicas(name, FunctionState.RUNNING)) > 0:
            q.popleft()
            yield from self.scale_down(name, 1)
        self.downscale_in_progress[name] = False

    def scale_down(self, fn_name: str, remove: int):
        replica_count = len(self.get_replicas(fn_name, FunctionState.RUNNING))
        if replica_count == 0:
            return
        replica_count -= remove
        if replica_count <= 0:
            remove = remove + replica_count

        scale_min = self.functions_deployments[fn_name].scaling_config.scale_min
        cnt = len(self.get_replicas(fn_name))
        if cnt - remove < scale_min:
            remove = cnt - scale_min

        replica_count = len(self.get_replicas(fn_name, FunctionState.RUNNING))
        if replica_count - remove < 0 or remove == 0:
            return

        logger.info(f'scale down {fn_name} by {remove}')
        replicas = self.choose_replicas_to_remove(fn_name, remove)
        self.env.metrics.log_scaling(fn_name, -remove)
        for replica in replicas:
            yield from self._remove_replica(replica)

    def _remove_replica(self, replica):
        env = self.env
        node = replica.node.skippy_node
        logger.info('removing pod {}'.format(replica.pod.name))

        env.metrics.log_teardown(replica)
        yield from replica.simulator.teardown(env, replica)

        assert replica.pod in node.pods
        self.env.cluster.remove_pod_from_node(replica.pod, node)
        replica.state = FunctionState.SUSPENDED
        self.replicas[replica.function.name].remove(replica)

        env.metrics.log('allocation', {
            'cpu': 1 - (node.allocatable.cpu_millis / node.capacity.cpu_millis),
            'mem': 1 - (node.allocatable.memory / node.capacity.memory)
        }, node=node.name)
        self.replica_count[replica.fn_name] -= 1
        self.functions_definitions[replica.image] -= 1

    def run_scheduler_worker(self):
        env = self.env

        while True:
            replica: FunctionReplica
            replica, services = yield self.scheduler_queue.get()

            logger.debug('scheduling next replica %s', replica.function.name)

            # schedule the required pod
            self.env.metrics.log_start_schedule(replica)
            pod = replica.pod
            then = time.time()
            result = env.scheduler.schedule(pod)
            duration = time.time() - then
            self.env.metrics.log_finish_schedule(replica, result)

            yield env.timeout(duration)  # include scheduling latency in simulation time

            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('Pod scheduling took %.2f ms, and yielded %s', duration * 1000, result)

            if not result.suggested_host:
                self.replicas[replica.fn_name].remove(replica)
                if len(services) > 0:
                    logger.warning('retry scheduling pod %s', pod.name)
                    yield from self.deploy_replica(replica.function, services[0], services[1:])
                else:
                    logger.error('pod %s cannot be scheduled', pod.name)
                continue

            logger.info('pod %s was scheduled to %s', pod.name, result.suggested_host)

            replica.node = self.env.get_node_state(result.suggested_host.name)
            node = replica.node.skippy_node

            env.metrics.log('allocation', {
                'cpu': 1 - (node.allocatable.cpu_millis / node.capacity.cpu_millis),
                'mem': 1 - (node.allocatable.memory / node.capacity.memory)
            }, node=node.name)

            self.functions_definitions[replica.image] += 1
            self.replica_count[replica.fn_name] += 1

            self.env.metrics.log_function_deploy(replica)
            env.process(simulate_function_start(env, replica))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('data_dir')
    args = parser.parse_args()
    main(Path(args.data_dir))

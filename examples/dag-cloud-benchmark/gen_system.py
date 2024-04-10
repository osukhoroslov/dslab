import argparse

from dataclasses import dataclass
from pathlib import Path


@dataclass
class VM:
    name: str
    speed: float
    vcpu: int
    memory: int
    price: float


DATA = [
    VM(name='m5.large', speed=3.1, vcpu=2, memory=8, price=0.096),
    VM(name='m5.xlarge', speed=3.1, vcpu=4, memory=16, price=0.192),
    VM(name='m5.2xlarge', speed=3.1, vcpu=8, memory=32, price=0.384),
    VM(name='m5.4xlarge', speed=3.1, vcpu=16, memory=64, price=0.768),
    VM(name='c5.large', speed=3.6, vcpu=2, memory=4, price=0.085),
    VM(name='c5.xlarge', speed=3.6, vcpu=4, memory=8, price=0.17),
    VM(name='c5.2xlarge', speed=3.6, vcpu=8, memory=16, price=0.34),
    VM(name='c5.4xlarge', speed=3.6, vcpu=16, memory=32, price=0.68),
]

def main(out_path, par):
    with open(out_path, 'w') as f:
        f.write('resources:\n')
        for d in DATA:
            for i in range(par):
                f.write(f'  - name: {d.name}_vm{i}\n')
                f.write(f'    speed: {d.speed * 1000 * d.vcpu}\n')
                f.write(f'    cores: 1\n')
                f.write(f'    memory: {d.memory * 1024}\n')
                f.write(f'    price: {d.price}\n')
        f.write('network:\n')
        f.write('  model: ConstantBandwidth\n')
        f.write('  bandwidth: 5000\n')
        f.write('  latency: 0\n')
        

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--parallelization', type=int)
    parser.add_argument('out', type=Path)
    args = parser.parse_args()
    main(args.out, args.parallelization)

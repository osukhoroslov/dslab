# This file creates big function traces from small function traces.
# It requires a function trace in OpenDC format.
# Some function traces can be found here: https://github.com/JOUNAIDSoufiane/OpenDC-Serverless/tree/master/experiment-analysis/resources/lambda/traces
import argparse
import sys


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='generate trace from sample')
    parser.add_argument('invocations', type=int, help='number of invocations')
    args = parser.parse_args()

    n = args.invocations

    lines = list(sys.stdin.readlines())
    sys.stdout.write(lines[0])
    max_time = int(lines[-1].split(',')[0])
    init = []
    for line in lines[1:]:
        tokens = line.split(',')
        tokens[0] = int(tokens[0])
        tokens[1] = int(tokens[1])
        init.append(tokens)

    t = 0
    ptr = 0
    inv = 0
    while inv < n:
        want = min(init[ptr][1] + 1, n - inv)
        inv += want
        row = init[ptr].copy()
        row[1] = str(want)
        row[0] += t
        if ptr + 1 == len(init):
            t = row[0]
            ptr = 0
        else:
            ptr += 1 
        row[0] = str(row[0])
        sys.stdout.write(','.join(row))

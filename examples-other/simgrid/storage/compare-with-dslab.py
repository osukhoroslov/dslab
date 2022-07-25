#!/usr/bin/env python3

import subprocess
import argparse
import os
import re
import numpy as np


DSLAB_BINARY_PATH = "target/release/storage-disk-benchmark"
DSLAB_ADDITIONAL_ARGS = None

SIMGRID_BINARY_PATH = "examples-other/simgrid/build/relwithdebinfo/bin/storage"
SIMGRID_ADDITIONAL_ARGS = "--log=root.thres:info"


class RequestInfo:
    def start(self, idx: int, disk_idx: int, size: int, expected_start_time: float, real_start_time: float):
        self.idx = idx
        self.disk_idx = disk_idx
        self.size = size
        self.expected_start_time = expected_start_time
        self.real_start_time = real_start_time

    def complete(self, complete_time: float, elapsed_time: float):
        self.complete_time = complete_time
        self.elapsed_time = elapsed_time


def get_data(lines, on_start_regex, on_complete_regex):
    requests = dict()
    n_completed = 0

    for line in lines:
        m = on_start_regex.search(line)
        if m:
            real_start_time, request_idx, disk_idx, size, expected_start_time = float(
                m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4)), float(m.group(5))
            assert(request_idx not in requests)
            req = RequestInfo()
            req.start(request_idx, disk_idx, size,
                      expected_start_time, real_start_time)
            requests[request_idx] = req
            continue
        m = on_complete_regex.search(line)
        if m:
            complete_time, request_idx, disk_idx, size, elapsed_time = float(m.group(
                1)), int(m.group(2)), int(m.group(3)), int(m.group(4)), float(m.group(5))
            assert(request_idx in requests)
            assert(disk_idx == requests[request_idx].disk_idx)
            assert(size == requests[request_idx].size)
            requests[request_idx].complete(complete_time, elapsed_time)
            n_completed += 1

    assert(len(requests) == n_completed)
    return requests


def get_simgrid_data(lines):
    on_start_regex = re.compile(
        "\[sample\_host:runner:\(2\) ([\d\.]+)\] \[disk\_test\/INFO\] Starting request #(\d+): read from disk\-([\d]+), size = ([\d]+), expected start time = ([\d\.]+)")
    on_complete_regex = re.compile(
        "\[sample\_host:runner:\(2\) ([\d\.]+)\] \[disk\_test\/INFO\] Completed request #(\d+): read from disk\-([\d]+), size = ([\d]+), elapsed simulation time = ([\d\.]+)")
    requests = get_data(lines, on_start_regex, on_complete_regex)
    print("SimGrid requests count:", len(requests))
    return requests


def get_dslab_data(lines):
    on_start_regex = re.compile(
        "\[([\d\.]+) INFO  [\w\-]+] Starting request #(\d+): read from disk\-([\d]+), size = ([\d]+), expected start time = ([\d\.]+)")
    on_complete_regex = re.compile(
        "\[([\d\.]+) INFO  [\w\-]+] Completed request #(\d+): read from disk\-([\d]+), size = ([\d]+), elapsed simulation time = ([\d\.]+)")
    requests = get_data(lines, on_start_regex, on_complete_regex)
    print("DSLab requests count:", len(requests))
    return requests


def print_analysis(x, y):
    print("\tAbsolute diff")
    absolute_diff = np.abs(x - y)
    print(f"\tMin: {np.min(absolute_diff)}")
    print(f"\tAvg: {np.average(absolute_diff)}")
    print(f"\tMax: {np.max(absolute_diff)}")
    print()
    print("\tRelative diff")
    relative_diff = absolute_diff / np.abs(x)
    print(f"\tMin: {np.min(relative_diff)}")
    print(f"\tAvg: {np.average(relative_diff)}")
    print(f"\tMax: {np.max(relative_diff)}")


def compare_data(dslab_data, simgrid_data, expected_size):
    if not (len(dslab_data) == len(simgrid_data) == expected_size):
        print("Sizes not equal")
        return False

    sorted_dslab_requests = sorted([dslab_data[request_idx] for request_idx in dslab_data],
                                   key=lambda request: request.idx)
    sorted_simgrid_requests = sorted([simgrid_data[request_idx] for request_idx in simgrid_data],
                                     key=lambda request: request.idx)

    dslab_expected_start_times, dslab_real_start_times = \
        np.array([r.expected_start_time for r in sorted_dslab_requests]), \
        np.array([r.real_start_time for r in sorted_dslab_requests])

    simgrid_expected_start_times, simgrid_real_start_times = \
        np.array([r.expected_start_time for r in sorted_simgrid_requests]), \
        np.array([r.real_start_time for r in sorted_simgrid_requests])

    dslab_complete_times, simgrid_complete_times = \
        np.array([r.complete_time for r in sorted_dslab_requests]), \
        np.array([r.complete_time for r in sorted_simgrid_requests])

    dslab_elapsed_times, simgrid_elapsed_times = \
        np.array([r.elapsed_time for r in sorted_dslab_requests]), \
        np.array([r.elapsed_time for r in sorted_simgrid_requests])

    dslab_start_times, simgrid_start_times = \
        np.array([r.real_start_time for r in sorted_dslab_requests]), \
        np.array([r.real_start_time for r in sorted_simgrid_requests])

    print()
    print("DSLab request start real vs expected times")
    print_analysis(dslab_real_start_times, dslab_expected_start_times)
    print()
    print("SimGrid request start real vs expected times")
    print_analysis(simgrid_real_start_times, simgrid_expected_start_times)
    print()
    print("Request start times")
    print_analysis(dslab_start_times, simgrid_start_times)
    print()
    print("Request complete times")
    print_analysis(dslab_complete_times, simgrid_complete_times)
    print()
    print("Request elapsed times")
    print_analysis(dslab_elapsed_times, simgrid_elapsed_times)

    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--requests", type=int, required=True)
    ap.add_argument("--disks", type=int, required=True)
    ap.add_argument("--max-size", type=int, required=True)
    ap.add_argument("--max-start-time", type=int, required=True)
    args = ap.parse_args()

    def run(executable_path, additional_args):
        command = [os.getenv("DSLAB_BASE_DIR", "") + "/" + executable_path, "--requests", str(args.requests),
                   "--disks", str(args.disks), "--max-size", str(args.max_size), "--max-start-time", str(args.max_start_time)]
        if additional_args:
            command.append(additional_args)

        joined_command = " ".join(command)
        print(f"Running: \"{joined_command}\"")

        # for DSLab
        my_env = os.environ.copy()
        my_env["RUST_LOG"] = "Debug"

        return subprocess.run(command, env=my_env, capture_output=True, text=True).stderr.split('\n')

    if not compare_data(
        get_dslab_data(run(DSLAB_BINARY_PATH, DSLAB_ADDITIONAL_ARGS)),
        get_simgrid_data(run(SIMGRID_BINARY_PATH, SIMGRID_ADDITIONAL_ARGS)),
        args.requests
    ):
        exit(1)


if __name__ == "__main__":
    main()

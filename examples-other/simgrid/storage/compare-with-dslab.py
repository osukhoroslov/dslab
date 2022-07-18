#!/usr/bin/env python3

import subprocess
import argparse
import os
import re
import numpy as np


DSLAB_BINARY_PATH = "target/release/storage-disk-bench"
DSLAB_ADDITIONAL_ARGS = None

SIMGRID_BINARY_PATH = "examples-other/simgrid/build/relwithdebinfo/bin/storage"
SIMGRID_ADDITIONAL_ARGS = "--log=root.thres:info"


def get_simgrid_data(lines):
    regex = re.compile(
        "\[sample\_host:runner:\(2\) ([\d\.]+)\] \[disk\_test\/INFO\] Completed reading from disk\-([\d]+), size = ([\d]+)")
    data = []
    for line in lines:
        m = regex.match(line)
        if m is not None:
            data.append((float(m.group(1)), int(m.group(2)), int(m.group(3))))
    print("SimGrid data size:", len(data))
    return data


def get_dslab_data(lines):
    regex = re.compile(
        "\[([\d\.]+) DEBUG [\w\-]+] Completed reading from disk\-([\d]+), size = ([\d]+)")
    data = []
    for line in lines:
        m = regex.match(line)
        if m is not None:
            data.append((float(m.group(1)), int(m.group(2)), int(m.group(3))))
    print("DSLab data size:", len(data))
    return data


def compare_data(x, y, expected_size):
    if not (len(x) == len(y) == expected_size):
        print("Sizes not equal")
        return False

    time_diff = []
    for i in range(len(x)):
        if x[i][1] != y[i][1] or x[i][2] != y[i][2]:
            print(f"Order mismatch: {x[i]} != {y[i]}")
            return False
        time_diff.append(abs(x[i][0] - y[i][0]))

    time_diff = np.array(time_diff)

    print("Time series analysis:")
    print(f"Avg: {np.average(time_diff)}")
    print(f"Max: {np.max(time_diff)}")
    print(f"Std: {np.std(time_diff)}")

    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--requests", type=int, required=True)
    ap.add_argument("--disks", type=int, required=True)
    ap.add_argument("--max-size", type=int, required=True)
    ap.add_argument("--max-start-time", type=int, required=True)
    args = ap.parse_args()

    def run(binary, additional_args):
        command = [os.getenv("DSLAB_BASE_DIR", "") + "/" + binary, "--requests", str(args.activities),
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
        args.activities
    ):
        exit(1)


if __name__ == "__main__":
    main()

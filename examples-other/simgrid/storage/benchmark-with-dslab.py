#!/usr/bin/env python3

import subprocess
import argparse
import os
import re
import numpy as np


DSLAB_BINARY_PATH = "target/release/storage-disk-benchmark"
DSLAB_ADDITIONAL_ARGS = None

SIMGRID_BINARY_PATH = "examples-other/simgrid/build/relwithdebinfo/bin/storage"
SIMGRID_ADDITIONAL_ARGS = "--log=root.thres:warning"


SIMGRID_REGEX = re.compile("Done\. Elapsed ([\d]+) ms")
DSLAB_REGEX = re.compile("Processed \d+ requests in (\d+) ms")


def get_simgrid_data(lines):
    for line in lines:
        m = SIMGRID_REGEX.search(line)
        if m is not None:
            return int(m.group(1))
    return None


def get_dslab_data(lines):
    for line in lines:
        m = DSLAB_REGEX.search(line)
        if m is not None:
            return int(m.group(1))
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--requests-list", required=True)
    ap.add_argument("--disks-list", required=True)
    ap.add_argument("--max-size", type=int, required=True)
    ap.add_argument("--max-start-time", type=int, required=True)
    args = ap.parse_args()

    def run(binary, additional_args, requests, disks, use_stderr=False):
        command = [os.getenv("DSLAB_BASE_DIR", "") + "/" + binary, "--requests", str(requests),
                   "--disks", str(disks), "--max-size", str(args.max_size), "--max-start-time", str(args.max_start_time)]
        if additional_args:
            command.append(additional_args)

        joined_command = " ".join(command)
        # print(f"Running: \"{joined_command}\"")

        if use_stderr:
            return subprocess.run(command, capture_output=True, text=True).stderr.split('\n')
        return subprocess.run(command, capture_output=True, text=True).stdout.split('\n')

    def eval(requests, disks):
        print(f"Reqs: {requests}, Disks: {disks}, ", end="")
        dslab_time = get_dslab_data(
            run(DSLAB_BINARY_PATH, DSLAB_ADDITIONAL_ARGS, requests, disks))
        simgrid_time = get_simgrid_data(
            run(SIMGRID_BINARY_PATH, SIMGRID_ADDITIONAL_ARGS, requests, disks, use_stderr=True))
        print(f"Dslab: {dslab_time}, SimGrid: {simgrid_time}")

    requests_list = args.requests_list.split(",")
    disks_list = args.disks_list.split(",")
    for requests in requests_list:
        for disks in disks_list:
            eval(requests, disks)


if __name__ == "__main__":
    main()

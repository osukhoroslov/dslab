import subprocess
import json
import os
import time

# Load config from JSON file
with open('config.json', 'r') as f:
    config = json.load(f)

# Create output directory if it doesn't exist
if not os.path.exists(config['output_dir']):
    os.makedirs(config['output_dir'])

# Initialize summary
summary = {
    'total': 0,
    'success': 0,
    'wrong': [],
    'non_zero_exit': [],
    'timed_out': []
}

# Run command with each argument
for arg in config['args']:
    # Construct command
    cmd = config['command'] + [arg]

    # Run command in subprocess and redirect stdout and stderr to separate log files
    stdout_file = os.path.join(config['output_dir'], arg + '_stdout.log')
    stderr_file = os.path.join(config['output_dir'], arg + '_stderr.log')
    print(cmd)
    with open(stdout_file, 'w') as stdout, open(stderr_file, 'w') as stderr:
        p = subprocess.Popen(cmd, stdout=stdout, stderr=stderr)

    # Wait for process to finish or time out
    start_time = time.time()
    while True:
        if p.poll() is not None:
            # Process has finished
            break
        elif time.time() - start_time > config['timeout']:
            print(time.time(), start_time)
            # Process has timed out
            p.kill()
            summary['timed_out'].append(arg)
            break
        else:
            time.sleep(1)

    # Check exit code
    if p.returncode == 0:
        summary['success'] += 1
    elif p.returncode == 1:
        summary['wrong'].append(arg)
    else:
        summary['non_zero_exit'].append(arg)

    # Write log file
    log_file = os.path.join(config['output_dir'], arg + '.log')
    with open(log_file, 'w') as f:
        f.write(f"Command: {' '.join(cmd)}\n")
        f.write(f"Exit code: {p.returncode}\n")
        f.write(f"Time taken: {time.time() - start_time:.2f} seconds\n")
        f.write(f"Stdout: {stdout_file}\n")
        f.write(f"Stderr: {stderr_file}\n")

    # Increment total count
    summary['total'] += 1

# Write summary file
summary_file = os.path.join(config['output_dir'], 'summary.txt')
with open(summary_file, 'w') as f:
    f.write(f"Total runs: {summary['total']}\n")
    f.write(f"Successful runs: {summary['success']}\n")
    f.write(f"Wrong runs: {summary['wrong']}\n")
    f.write(f"Failed runs (non-zero exit code): {summary['non_zero_exit']}\n")
    f.write(f"Failed runs (timed out): {summary['timed_out']}\n")

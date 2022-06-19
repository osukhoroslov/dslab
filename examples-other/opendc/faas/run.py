import argparse
import subprocess
import time


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='run benchmark')
    parser.add_argument('-opendc-harness', type=str, help='path to opendc-harness script')
    parser.add_argument('-opendc-serverless', type=str, help='path to serverless experiment jar')
    parser.add_argument('-trace', type=str, help='path to trace directory')
    parser.add_argument('-save-config', type=str, default='c.conf', help='path to save experiment config')
    args = parser.parse_args()
    with open(args.save_config, 'w') as f:
        f.write('opendc.experiments.serverless20 {{\n\ttrace-path = {}\n}}\n'.format(args.trace))
    print('running with command', ' '.join([args.opendc_harness, '--class-path', args.opendc_serverless, '-p', '1', '-c', args.save_config, 'Serverless']))
    begin = time.time()
    subprocess.run([args.opendc_harness, '--class-path', args.opendc_serverless, '-p', '1', '-c', args.save_config, 'Serverless'], check=True)
    print(time.time() - begin, ' seconds')

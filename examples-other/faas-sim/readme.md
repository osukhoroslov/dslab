# FaaS-Sim Serverless Example
This example implements a benchmark for [FaaS-Sim](https://github.com/edgerun/faas-sim/tree/master). The benchmark shares common scenario with OpenDC FaaS benchmark in the sibling directory and with `faas-benchmark` in `examples`. The dataset should be a directory of .csv files that contain function traces in OpenDC format, one file for each function. Refer to `faas-benchmark` if you want to generate data for benchmarks.
## Running
1. Get FaaS-Sim [source code](https://github.com/edgerun/faas-sim/tree/master).
2. Install all dependencies from `requirements.txt` and check that the simulator runs. 
**Important note:** currently (May 2023) there are some problems with running FaaS-Sim on recent Python versions, this example was run using Python 3.8 in [pyenv](https://github.com/pyenv/pyenv).
3. Make a directory inside `examples` directory (e. g. `faas-sim/examples/benchmark`) and copy all .py files from this directory to the new directory.
4. Run `python3 -m examples.%your_directory%.main %path_to_data%` from faas-sim root directory.

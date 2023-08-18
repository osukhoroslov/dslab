# OpenDC Serverless Example
This example implements a benchmark for [OpenDC 2.0 FaaS](https://github.com/atlarge-research/opendc/releases/tag/v2.0). The benchmark shares common scenario with FaaS-Sim benchmark in the sibling directory and with `faas-benchmark` in `examples`. The dataset should be a directory of .csv files that contain function traces in OpenDC format, one file for each function. Refer to `faas-benchmark` if you want to generate data for benchmarks.

## Running
1. Get OpenDC 2.0 [source code](https://github.com/atlarge-research/opendc/releases/tag/v2.0).
2. Patch it: replace [this line](https://github.com/atlarge-research/opendc/blob/6d2b140311057e54622fdcd6cf7f8850c370414c/opendc-serverless/opendc-serverless-service/src/main/kotlin/org/opendc/serverless/service/FunctionObject.kt#L132) with
```
val copy = instances.toList()
copy.forEach(FunctionInstance::close)
```
3. Replace `opendc-experiments/opendc-experiments-serverless20/src/main/kotlin/org/opendc/experiments/serverless/ServerlessExperiment.kt` with the corresponding file from this directory.
4. Build it: go to the root directory and run `./gradlew installDist`. Directory `build/install/opendc` will contain opendc-harness and experiment jar required for running the benchmark.
5. Run the benchmark with `run.py`. **Note**: for large benchmark datasets it is advised to set `DEFAULT_JAVA_OPTS="-XX:+UseConcMarkSweepGC -Xmx10g"` in opendc-harness to avoid crashes.

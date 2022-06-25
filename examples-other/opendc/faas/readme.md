# OpenDC Serverless Example
This is a serverless example from OpenDC 2.0 with minor changes that make it easier to benchmark performance.

## Running
1. Get OpenDC 2.0 [source code](https://github.com/atlarge-research/opendc/releases/tag/v2.0).
2. Patch it: replace [this line](https://github.com/atlarge-research/opendc/blob/6d2b140311057e54622fdcd6cf7f8850c370414c/opendc-serverless/opendc-serverless-service/src/main/kotlin/org/opendc/serverless/service/FunctionObject.kt#L132) with
```
val copy = instances.toList()
copy.forEach(FunctionInstance::close)
```
3. Replace `ServerlessExperiment.kt` with file from this directory.
4. Generate trace with `generate.py` (benchmark in the paper was generated from frequent-noconcurrency.csv and timer-noconcurrency.csv from [here](https://github.com/JOUNAIDSoufiane/OpenDC-Serverless/tree/master/experiment-analysis/resources/lambda/traces)).
5. Run experiment with `run.py`.

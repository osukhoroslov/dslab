# FaaS scheduling experiment
This crate experiments with several simple scheduling strategies and [Hermes](https://arxiv.org/abs/2111.07226) scheduler.
## Running experiment
- download and unpack Azure functions [dataset](https://github.com/Azure/AzurePublicDataset/blob/master/AzureFunctionsDataset2019.md)
- build
- run `faas-scheduling-experiment %path_to_dataset%`

It is recommended to build strictly in release mode and leave only one day out of 14 since the dataset is really large.
Note that the last two days in the dataset have no memory percentiles. Such days are ignored.

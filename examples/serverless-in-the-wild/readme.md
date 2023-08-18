# Serverless in the Wild reproduction
This crate reproduces the experiments with keepalive policies from [Serverless in the Wild](https://www.usenix.org/conference/atc20/presentation/shahrad) paper.

Note that console output is just a default description of simulation results, for paper-related results you should specify `--plot-metrics` and `--plot-cdf` options. In this case the program will make a plot of relevant metrics and a plot of cold start CDF.
## Steps to reproduce
- download and unpack Azure functions [dataset](https://github.com/Azure/AzurePublicDataset/blob/master/AzureFunctionsDataset2019.md)
- build
- run `serverless-in-the-wild %path_to_dataset% --config %config% --plot-metrics %output_file_for_metrics% --plot-cdf %output_file_for_cdf%`

It is recommended to build strictly in release mode and leave only one day out of 14 since the dataset is really large.
Note that the last two days in the dataset have no memory percentiles. Such days are ignored.

# Serverless in the Wild reproduction
This crate reproduces the experiments with keepalive policies from [Serverless in the Wild](https://www.usenix.org/conference/atc20/presentation/shahrad) paper.
## Steps to reproduce
- download and unpack Azure functions [dataset](https://github.com/Azure/AzurePublicDataset/blob/master/AzureFunctionsDataset2019.md)
- build
- run `serverless-in-the-wild %path_to_dataset% %config%`

It is recommended to build strictly in release mode and leave only one day out of 14 since the dataset is really large.
Note that the last two days in the dataset have no memory percentiles. Such days are ignored.

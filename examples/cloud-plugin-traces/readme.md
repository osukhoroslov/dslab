### Parsing Azure Trace for Packing 2020

Steps to reproduce:
1) Donwload sqlite dataset from https://github.com/Azure/AzurePublicDataset/blob/master/AzureTracesForPacking2020.md
2) Convert vm table using following:
```
$ sqlite3 packing_trace_zone_a_v1.sqlite
sqlite> .headers on
sqlite> .mode csv
sqlite> .output vm_instances.csv
sqlite> SELECT vmId, vmTypeId, starttime, endtime FROM vm ORDER BY starttime;
sqlite> .quit
````
3) Convert vmType table:
```
$ sqlite3 packing_trace_zone_a_v1.sqlite
sqlite> .headers on
sqlite> .mode csv
sqlite> .output vm_types.csv
sqlite> SELECT id, vmTypeId, core, memory FROM vmType;
sqlite> .quit
````
4) Setup `dataset_type` variable to `DatasetType::Azure` in `main.rs`.
5) Put these files to this directory and run `RUST_LOG=info cargo run`

### Parsing Huawei VM Placements Dataset

1) Download dataset from https://github.com/huaweicloud/VM-placement-dataset/blob/main/Huawei-East-1/data/Huawei-East-1.csv
2) Put the file to this directory and run `RUST_LOG=info cargo run`
3) Do not forget to keep `dataset_type=DatasetType::Huawei` 

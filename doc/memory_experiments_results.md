# Results from the memory experiments

## Used Datasets

All datasets were generated using the [`generateData.sh`-script](../scripts/generateData.sh).
We use four different data setups to compare memory overhead of our framework.
We compare the memory usage of the example application, the same data loaded into memory as one string or within Relations.


- Dataset `data_010_mb`

  Overview about the number of dactors and relations in this dataset:
  
  |  | #dactors | #relations / dactor | #relations | disk size |
  |:-|---------:|--------------------:|-----------:|----------:|
  | Cart | 573 | 2 | 1146 |  |
  | Customer | 62 | 3 | 186 |  |
  | GroupManager | 6 | 1 | 6 |  |
  | StoreSection | 188 | 2 | 376 |  |
  | **Summary** | 829 |  | 1714 | 10M |

- Dataset `data_025_mb`

  Overview about the number of dactors and relations in this dataset:
  
    |  | #dactors | #relations / dactor | #relations | disk size |
    |:-|---------:|--------------------:|-----------:|----------:|
    | Cart | 2221 | 2 | 4442 |  |
    | Customer | 100 | 3 | 300 |  |
    | GroupManager | 6 | 1 | 6 |  |
    | StoreSection | 251 | 2 | 502 |  |
    | **Summary** | 2578 |  | 5250 | 25M |
  
- Dataset `data_050_mb`

  Overview about the number of dactors and relations in this dataset:
  
  |  | #dactors | #relations / dactor | #relations | disk size |
  |:-|---------:|--------------------:|-----------:|----------:|
  | Cart | 3661 | 2 | 7322 |  |
  | Customer | 200 | 3 | 600 |  |
  | GroupManager | 11 | 1 | 11 |  |
  | StoreSection | 501 | 2 | 1002 |  |
  | **Summary** | 4373 |  | 8935 | 50M |

  
- Dataset `data_100_mb`

  Overview about the number of dactors and relations in this dataset:
  
  |  | #dactors | #relations / dactor | #relations | disk size |
  |:-|---------:|--------------------:|-----------:|----------:|
  | Cart | 7264 | 2 | 14528 |  |
  | Customer | 381 | 3 | 1143 |  |
  | GroupManager | 21 | 1 | 21 |  |
  | StoreSection | 952 | 2 | 1904 |  |
  | **Summary** | 8618 |  | 17596 | 100M |

## Results

| Dataset       | Method                |
|:--------------|:----------------------|
| `data_010_mb` | single string in mem. |
| `data_025_mb` | single string in mem. |
| `data_050_mb` | single string in mem. |
| `data_100_mb` | single string in mem. |

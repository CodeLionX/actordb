# Results from the memory experiments

## Used Datasets

All datasets were generated using the [`generateData.sh`-script](../scripts/generateData.sh).
We use four different data setups to compare memory overhead of our framework.
We compare the memory usage of the example application, the same data loaded into memory as one string or within Relations.

See pull request [#113](https://github.com/CodeLionX/actordb/pull/113) for details, how the memory was measured.

- Dataset `data_010_mb`

  Overview about the number of dactors and relations in this dataset:
  
  |              | #dactors | #relations / dactor | #relations | disk size |
  |:-------------|---------:|--------------------:|-----------:|----------:|
  | Cart         |      573 |                   2 |       1146 |           |
  | Customer     |       62 |                   3 |        186 |           |
  | GroupManager |        6 |                   1 |          6 |           |
  | StoreSection |      188 |                   2 |        376 |           |
  | **Summary**  |      829 |                     |       1714 |      10 M |

- Dataset `data_025_mb`

  Overview about the number of dactors and relations in this dataset:
  
  |              | #dactors | #relations / dactor | #relations | disk size |
  |:-------------|---------:|--------------------:|-----------:|----------:|
  | Cart         |     2221 |                   2 |       4442 |           |
  | Customer     |      100 |                   3 |        300 |           |
  | GroupManager |        6 |                   1 |          6 |           |
  | StoreSection |      251 |                   2 |        502 |           |
  | **Summary**  |     2578 |                     |       5250 |      25 M |
  
- Dataset `data_050_mb`

  Overview about the number of dactors and relations in this dataset:
  
  |              | #dactors | #relations / dactor | #relations | disk size |
  |:-------------|---------:|--------------------:|-----------:|----------:|
  | Cart         |     3661 |                   2 |       7322 |           |
  | Customer     |      200 |                   3 |        600 |           |
  | GroupManager |       11 |                   1 |         11 |           |
  | StoreSection |      501 |                   2 |       1002 |           |
  | **Summary**  |     4373 |                     |       8935 |      50 M |

  
- Dataset `data_100_mb`

  Overview about the number of dactors and relations in this dataset:
  
  |              | #dactors | #relations / dactor | #relations | disk size |
  |:-------------|---------:|--------------------:|-----------:|----------:|
  | Cart         |     7264 |                   2 |      14528 |           |
  | Customer     |      381 |                   3 |       1143 |           |
  | GroupManager |       21 |                   1 |         21 |           |
  | StoreSection |      952 |                   2 |       1904 |           |
  | **Summary**  |     8618 |                     |      17596 |     100 M |

## Results

### Single string in memory

We measured the retained size of the one `StringHolder`-instance in a heap dump after GC.

| Dataset       | retained size | rounded size |
|:--------------|--------------:|-------------:|
| `data_010_mb` |    11315812 B |      10.8 MB |
| `data_025_mb` |    18637898 B |      17.8 MB |
| `data_050_mb` |    49246330 B |      47.0 MB |
| `data_100_mb` |    98492560 B |      93.9 MB |

### Relations in memory

We measured the retained size of all `RowRelation`-instances in a heap dump after GC.
The number of `RowRelation`-instances corresponds with the number of relations (_#relations_).

| Dataset       | #relations | retained size | rounded size |
|:--------------|-----------:|--------------:|-------------:|
| `data_010_mb` |       1714 |    29478612 B |      28.1 MB |
| `data_025_mb` |       5250 |    45091303 B |      43.0 MB |
| `data_050_mb` |       8935 |   121942297 B |     116.3 MB |
| `data_100_mb` |      17596 |    B |      MB |

### Full framework with dactors

We measured the retained sizes of all instances of `Cart`-, `Customer`-, `GroupManager`- and `StoreSection`-Dactors in a heap dump after GC.
In the table below we report the summarized sizes of all dactor instances.

| Dataset       | #dactors  | #relations | retained size | rounded size |
|:--------------|----------:|-----------:|--------------:|-------------:|
| `data_010_mb` |       829 |       1714 |    29914591 B |      28.5 MB |
| `data_025_mb` |      2578 |       5250 |    46481385 B |      44.3 MB |
| `data_050_mb` |      4373 |       8935 |   124267240 B |     118.5 MB |
| `data_100_mb` |      8618 |      17596 |   238456342 B |     227.4 MB |

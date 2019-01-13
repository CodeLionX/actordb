# Results from the performance experiments

## Used Datasets

See [Memory Benchmarks: Used Datasets](./memory_experiments_results.md#used-datasets).

## Results

### Scan query results (throughput)

- dataset: `data_010_mb`

  We measured the time needed to scan over the Inventory relation of the 188 `StoreSection`-Dactors.
  
  Throughput: 894 Op/s

  ```
  ========== elapsed time (scan query) ==========
  elapsed nanos: 1118973561ns (1s)
  average time per query: 815850137ns
  Collected 1000 measurements
  Results written to results.txt
  ```

- dataset: `data_025_mb`

  We measured the time needed to scan over the Inventory relation of the 251 `StoreSection`-Dactors.

  Throughput: 738 Op/s

  ```
  ========== elapsed time (scan query) ==========
  elapsed nanos: 1355586170ns (1s)
  average time per query: 994861215ns
  Collected 1000 measurements
  Results written to results.txt
  ```

- dataset: `data_050_mb`

  We measured the time needed to scan over the Inventory relation of the 501 `StoreSection`-Dactors.

  Throughput: 356 Op/s

  ```
  ========== elapsed time (scan query) ==========
  elapsed nanos: 2806158077ns (2s)
  average time per query: 2324101657ns
  Collected 1000 measurements
  Results written to results.txt
  ```
  
- dataset: `data_100_mb`

  We measured the time needed to scan over the Inventory relation of the 1002 `StoreSection`-Dactors.

  Throughput: 133 Op/s

  ```
  ========== elapsed time (scan query) ==========
  elapsed nanos: 7526210316ns (7s)
  average time per query: 6969132794ns
  Collected 1000 measurements
  Results written to results.txt
  ```

### Point Query results (latency)

We measured the time in milliseconds needed to execute 1000 select queries for the Inventory relation of `StoreSection`-Dactors concurrently.
Results can be found in the files in the folder [`performance_experiments_results`](./performance_experiments_results) with a name starting with `point-query-results-*.txt`.
We pre-loaded the database with the `data_050_mb` dataset.

| `StorageManager`s |     Avg |  Median |
| ----------------: | ------: | ------: | 
|                50 |   22 ms |   25 ms | 
|               100 |   22 ms |   24 ms |
|               150 |   23 ms |   25 ms |
|               200 |   24 ms |   26 ms |
|               250 |   24 ms |   24 ms |
|               300 |   18 ms |   20 ms |
|               350 |   20 ms |   21 ms |
|               400 |   19 ms |   22 ms |
|               450 |   22 ms |   23 ms |
|               500 |   20 ms |   21 ms |
| **Summary:**      |   21 ms |   23 ms |

### Insert results

We measured the time needed to insert a record into the `Inventory` relation of the `StoreSection`-Dactors and the `Discounts` relation of the `GroupManager`-Dactors while inserting 1000 records concurrently.
Results can be found in the files in the folder [`performance_experiments_results`](./performance_experiments_results) with a name starting with `insert-results-*.txt`.
We pre-loaded the database with the `data_050_mb` dataset.

| `StoreSection`s | `GroupManager`s | Throughput |   Avg. | Median |
| --------------: | --------------: | ---------: | -----: | -----: |
|              50 |               1 |  8144 Op/s |  98 ms | 106 ms |
|             100 |               2 |  6873 Op/s | 111 ms | 113 ms |
|             150 |               3 |  7883 Op/s | 105 ms | 109 ms |
|             200 |               4 |  8120 Op/s | 103 ms | 107 ms |
|             250 |               5 |  7653 Op/s | 108 ms | 112 ms |
|             300 |               6 |  7644 Op/s | 103 ms | 107 ms |
|             350 |               7 |  8943 Op/s |  92 ms |  95 ms |
|             400 |               8 |  7753 Op/s | 106 ms | 109 ms |
|             450 |               9 |  8401 Op/s |  96 ms | 101 ms |
|             500 |              10 |  8017 Op/s | 107 ms | 111 ms |
| **Summary:**    |                 |  7943 Op/s | 103 ms | 107 ms |

The throughput just measures the external queries.
In reality the framework performs two inserts in two different actors for each external query received.
This means the system has an internal throughput of 15&nbsp;886 inserted records per second.

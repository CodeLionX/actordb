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

We measured the time needed to select data from the Inventory relation of one `StoreSection`-Dactor.

- dataset: `data_010_mb`

  ```
  ========== elapsed time (point query) ==========
  median latency per query: 381504.0ns
  Collected 1000 measurements
  Results written to point-query-results.txt
  ```

- dataset: `data_025_mb`

  ```
  ========== elapsed time (point query) ==========
  median latency per query: 368784.0ns
  Collected 1000 measurements
  Results written to point-query-results.txt
  ```

- dataset: `data_050_mb`

  ```
  ========== elapsed time (point query) ==========
  median latency per query: 369098.0ns
  Collected 1000 measurements
  Results written to point-query-results.txt
  ```
  
- dataset: `data_100_mb`

  ```
  ========== elapsed time (point query) ==========
  median latency per query: 357757.0ns
  Collected 1000 measurements
  Results written to point-query-results.txt
  ```

### Insert results

We measured the time needed to insert a record into the `Inventory` relation of the `StoreSection`-Dactor and the `Discounts` relation of the `GroupManager`-Dactor.

For the following experiments the load was distributed across 100 `StoreSection`s and 5 `GroupManager`s.

- dataset: `data_010_mb`

  ```
  ========== elapsed time (insert) ==========
  Inserted 1000 x 2 (2 Dactors) rows in 151321843 ns
  Throughput: 6608 Op/s
  Average latency: 9.6071328E7
  Median latency: 9.1940799E7
  Results written to insert-results.txt
  ```

- dataset: `data_025_mb`

  ```
  ========== elapsed time (insert) ==========
  Inserted 1000 x 2 (2 Dactors) rows in 140196826 ns
  Throughput: 7133 Op/s
  Average latency: 1.15048826E8
  Median latency: 1.1780474E8
  Results written to insert-results.txt
  ```

- dataset: `data_050_mb`

  ```
  ========== elapsed time (insert) ==========
  Inserted 1000 x 2 (2 Dactors) rows in 123986502 ns
  Throughput: 8065 Op/s
  Average latency: 8.9853722E7
  Median latency: 8.496072E7
  Results written to insert-results.txt
  ```

- dataset: `data_100_mb`

  ```
  ========== elapsed time (insert) ==========
  Inserted 1000 x 2 (2 Dactors) rows in 135505551 ns
  Throughput: 7380 Op/s
  Average latency: 1.12828333E8
  Median latency: 1.14831795E8
  Results written to insert-results.txt
  ```

For the following experiments we used the dataset `data_050_mb` and changed the number of Dactors.

- `StoreSection`s: 50, `GroupManager`s: 3

  ```
  ========== elapsed time (insert) ==========
  Inserted 1000 x 2 (2 Dactors) rows in 120123281 ns
  Throughput: 8324.780939008817 Op/s
  Average latency: 9.979085E7 ns
  Median latency: 1.01549892E8 ns
  Results written to insert-results.txt
  ```

- `StoreSection`s: 100, `GroupManager`s: 5

  ```
  ========== elapsed time (insert) ==========
  Inserted 1000 x 2 (2 Dactors) rows in 121623542 ns
  Throughput: 8222.092397210401 Op/s
  Average latency: 9.602113E7 ns
  Median latency: 9.5228632E7 ns
  Results written to insert-results.txt
  ```

- `StoreSection`s: 200, `GroupManager`s: 10


  ```
  ========== elapsed time (insert) ==========
  Inserted 1000 x 2 (2 Dactors) rows in 121047379 ns
  Throughput: 8261.228027085162 Op/s
  Average latency: 9.6566464E7 ns
  Median latency: 1.00438275E8 ns
  Results written to insert-results.txt
  ```

- `StoreSection`s: 400, `GroupManager`s: 10

  ```
  ========== elapsed time (insert) ==========
  Inserted 1000 x 2 (2 Dactors) rows in 121148480 ns
  Throughput: 8254.333855447463 Op/s
  Average latency: 1.02999358E8 ns
  Median latency: 1.05697289E8 ns
  Results written to insert-results.txt
  ```

  

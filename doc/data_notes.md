# Data Notes

We (at the moment) using 3 different data setups to compare memory overhead of
our framework. We compare the overhead with memory usage of example applications
loading that same data into memory as one string or within Relations.

## `data_small`

5.4 MB on disk

```bash
$ ./generateData.sh -n 50 -m 50000 -k 5 -o ../memorybenchmark/data_small
Generating data in CSV format for load testing:
  50 customers
  50000 items
  7500 purchases in 250 carts

Distributing data equaly to dactors:
  #customers = 50
    #storeVisits = 20
  #groups = 3
    #discounts = 2500
  #carts = 250
    #cart_purchases = 30
  #storeSections = 126
    #inventory = 400
    #purchase_history = 119
```

| Type                 | Size        |
|:---------------------|:------------|
| on disk              | 5,4 MB      |
| single string in-mem | 9,49 MB (9.953.941 bytes) |
| as relations in-mem  | 41,79 MB (43.821.150 bytes) |
| full framework       | 53,37 MB (55.963.368 bytes)| 

## `data` (medium)

```bash
$ ./generateData.sh -o ../memorybenchmark/data
Generating data in CSV format for load testing:
  100 customers
  100000 items
  30000 purchases in 2000 carts

Distributing data equaly to dactors:
  #customers = 100
    #storeVisits = 80
  #groups = 6
    #discounts = 5000
  #carts = 2000
    #cart_purchases = 15
  #storeSections = 251
    #inventory = 400
    #purchase_history = 239
```

| Type                 | Size        |
|:---------------------|:------------|
| on disk              | 24 MB       |
| single string in-mem | 21,78 MB (22,838,543 bytes) |
| as relations in-mem  | 106,92 MB (112,116,599 bytes) |
| full framework       | 130,50 MB (136,840,742 bytes) | 

## `data_big`

53 MB on disk

```bash
$ ./generateData.sh -n 200 -m 200000 -k 20 -o ../memorybenchmark/data_big
Generating data in CSV format for load testing:
  200 customers
  200000 items
  108000 purchases in 4000 carts

Distributing data equaly to dactors:
  #customers = 200
    #storeVisits = 80
  #groups = 11
    #discounts = 10000
  #carts = 4000
    #cart_purchases = 27
  #storeSections = 501
    #inventory = 400
    #purchase_history = 431
```

| Type                 | Size        |
|:---------------------|:------------|
| on disk              | 53 MB       |
| single string in-mem | 54,65 MB (57.301.100 bytes) |
| as relations in-mem  | 280,61 MB (294.244.616 bytes) |
| full framework       | 332,10 MB (348.200.913 bytes) | 

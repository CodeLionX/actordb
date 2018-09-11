# Results from the relation memory experiments

We generated data to be loaded into one relation to compare the relation overhead to the actual data size for different data sizes.

## Datasets

| Dataset         |     # items / # rows |  size on disk |
|:----------------|---------------------:|--------------:|
| `data-10000`    |          10&nbsp;000 |   364&nbsp;KB |
| `data-100000`   |         100&nbsp;000 |   4.3&nbsp;MB |
| `data-1000000`  |  1&nbsp;000&nbsp;000 |  49.0&nbsp;MB |
| `data-10000000` |  9&nbsp;999&nbsp;000 | 556.0&nbsp;MB |

## Results

We performed two experiments:
We loaded the data in one big `String` object and we used our `Relation` machinery to load the data into memory.

&nbsp;

In the following we report the retained sizes of the object in the heap:

| Dataset         |                `String` in B |     `String` rounded |              `Relation` in B |    `Relation` rounded |
|:----------------|-----------------------------:|---------------------:|-----------------------------:|----------------------:|
| `data-10000`    |                 724&nbsp;626 |          0.7&nbsp;MB |          1&nbsp;877&nbsp;256 |    1.8&nbsp;MB (259%) |
| `data-100000`   |          8&nbsp;644&nbsp;626 |          8.2&nbsp;MB |         18&nbsp;797&nbsp;256 |   17.9&nbsp;MB (217%) |
| `data-1000000`  |        102&nbsp;444&nbsp;628 |         97.7&nbsp;MB |        187&nbsp;997&nbsp;256 |  179.3&nbsp;MB (184%) |
| `data-10000000` | 1&nbsp;164&nbsp;326&nbsp;746 | 1&nbsp;110.4&nbsp;MB | 1&nbsp;879&nbsp;809&nbsp;444 | 1792.7&nbsp;MB (161%) |

If we have around 1&nbsp;GB of data loaded into one `Relation`, the overhead is reduced to around 60&nbsp;%. 
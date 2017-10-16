[![PyPI](https://img.shields.io/pypi/v/simtools.svg)](https://pypi.python.org/pypi/simtools)
[![Build Status](https://travis-ci.org/big-data-lab-team/sim.svg?branch=master)](https://travis-ci.org/big-data-lab-team/sim)

# Spark for neuroIMaging (sim)

A set of Spark MapReduce programs to process brain images (large datasets and/or large images)

## Installation

Dependencies:
* [Apache Spark pre-buit for Hadoop](http://spark.apache.org/downloads.html)
* [Docker](http://www.docker.com) (for BIDS apps)

Install the Python package:
* `pip install simtools`

## Spark BIDS Demo

Spark BIDS is a tool to process BIDS datasets with BIDS apps on a
Spark cluster, leveraging the "Map Reduce" model built in BIDS
apps. It supports participant and group analyses.

To run the demo:
```
spark_bids ./test/demo/bids-app-example.json ./test/demo/ds001 output
```

It should produce the following output:
```
Computed Analyses: Subject [ YES ] - Group [ YES ]
 [ SUCCESS ] sub-01
 [ SUCCESS ] sub-02
 [ SUCCESS ] group
```

A directory named `output` should also be created with the following content:
```
avg_brain_size.txt  sub-01_brain.nii.gz  sub-02_brain.nii.gz
```

The content of `avg_brain_size.txt` should be:
```
Average brain size is 830532 voxels
```

## Other examples

Other examples of Spark pipelines to process brain images are found in `other-examples`:
   - gen_histo.py  - histogram generation
   - bin_fsl.py - image binarization example using FSL installation in Docker container
   - bin_spark.py - image binarization without containerization. Testing difference between in-memory computing and writing to disk at each pipeline step  


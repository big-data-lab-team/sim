# mapreduce
A set of Spark MapReduce programs to process brain images (large datasets and/or large images)

Currently contains simple examples using Spark:
   - gen_histo.py  - histogram generation
   - bin_fsl.py - image binarization example using FSL installation in Docker container
   - bin_spark.py - image binarization without containerization. Testing difference between in-memory computing and writing to disk at each pipeline step  




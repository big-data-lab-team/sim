# Dependencies

* `pip install pybids`
* `pip install git+git://github.com/boutiques/boutiques.git@develop#subdirectory=tools/python`

# Demo

To run the demo:
```
spark-bids ./demo/bids-app-example.json ./demo/ds001 output
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


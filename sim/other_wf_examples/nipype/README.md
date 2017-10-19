# NipBIDS

An example of a nipype workflow executing sim functions / A demonstration of how to rewrite Spark BIDS in nipype

## Additional dependencies
* `pip install nipype`

## Demo
```
nip_bids ./sim/tests/demo/bids-app-example.json ./sim/tests/demo/ds001 output
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






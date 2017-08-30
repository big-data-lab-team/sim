# Demo

To run the demo, execute the following command *from this directory*:

`
spark-bids ./bids-app-example.json ./ds001 output
`

It should produce the following output:

`
Computed Analyses: Subject [ YES ] - Group [ YES ]

 [ SUCCESS ] sub-01
 
 [ SUCCESS ] sub-02
 
 [ SUCCESS ] group
`

A directory named `output` should also be created with the following content:

`
avg_brain_size.txt  sub-01_brain.nii.gz  sub-02_brain.nii.gz
`
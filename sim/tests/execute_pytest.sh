# !/bin/bash

# install sim
pip install -e ../


# sparkBIDS test
pytest --cov=./ tests

echo $PYTHONPATH
# nipBIDS test
pytest other_wf_examples/nipype/tests


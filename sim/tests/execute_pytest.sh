# !/bin/bash

export PYTHONPATH="/opt/conda/envs/python2/lib/python2.7/site-packages:/usr/local/spark-2.2.0-bin-hadoop2.7/python:/opt/conda/envs/python2/bin/"

# install sim
pip install -e ../


# sparkBIDS test
pytest --cov=./ tests

# nipBIDS test
pytest other_wf_examples/nipype/tests


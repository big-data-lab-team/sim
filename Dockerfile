FROM jupyter/pyspark-notebook:82b978b3ceeb

USER root

RUN apt-get update && \
    echo 'Y' | apt-get install apt-utils && \
    echo 'Y' | apt-get install curl && \
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add - && \
    echo 'Y' | apt install --reinstall base-files lsb-release lsb-base && \
    echo 'Y' | apt-get install software-properties-common && \
    echo 'Y' | apt-get install apt-transport-https && \
    add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $( lsb_release -cs ) stable" && \
    apt-get update && \
    apt-get install -y docker-ce && \
    service docker start

RUN conda create -n simenv python=2.7 pytest py4j==0.10.4  pyspark pytest-cov

ENV PATH /opt/conda/envs/simenv/bin:$PATH

RUN /bin/bash -c "source activate simenv"

ENV PYTHONPATH /opt/conda/envs/python2/lib/python2.7/site-packages:\
    /usr/local/spark-2.2.0-bin-hadoop2.7/python:\
    /opt/conda/envs/python2/bin:$PYTHONPATH

RUN pip install boutiques pybids duecredit nipype

ENTRYPOINT ["/bin/bash"]

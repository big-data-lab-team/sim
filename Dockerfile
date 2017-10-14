FROM mesosphere/spark:2.0.0-2.2.0-1-hadoop-2.6

RUN sudo apt-get update && apt-get -y upgrade && apt-get -y install docker.io \
    python-setuptools && \
    easy_install pip 



RUN pip install boutiques pytest pyspark pybids

ENTRYPOINT ["pytest"]

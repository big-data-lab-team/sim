FROM jupyter/pyspark-notebook:281505737f8a

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

RUN pip install boutiques pytest pyspark pybids duecredit

ENTRYPOINT ["pytest"]

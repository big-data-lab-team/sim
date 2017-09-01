FROM mesosphere/spark:2.0.0-2.2.0-1-hadoop-2.6

RUN apt-get -y install docker.io \
    python-setuptools && \
    easy_install pip \
    nodejs 

RUN apt-get -y install npm 

RUN npm install -g bids-validator

RUN pip install boutiques nodejs pytest simtools



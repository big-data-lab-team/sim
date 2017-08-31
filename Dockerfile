FROM p7hb/docker-spark

MAINTAINER https://github.com/big-data-lab-team

RUN apt-get update && \
    apt-get install -y python3 python3-pip nodejs && \
    pip3 install boutiques pybids pytest && \
    apt-get remove -y python3-pip && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY demo /root
	
COPY spark-bids.py /root

#RUN npm install -g bids-validator



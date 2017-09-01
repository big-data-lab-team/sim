FROM sequenceiq/spark:1.6.0

RUN yum -y install wget make tar xz-libs python-pip python-wheel

RUN yum install -y centos-release-SCL

RUN yum -y install epel-release && yum clean all

RUN wget http://curl.haxx.se/ca/cacert.pem && \
    mv cacert.pem ca-bundle.crt && \
    mv ca-bundle.crt /etc/pki/tls/certs

RUN wget http://www.python.org/ftp/python/2.7.6/Python-2.7.6.tar.xz && \
    xz -d Python-2.7.6.tar.xz && \
    tar -xvf Python-2.7.6.tar && \
    ./Python-2.7.6/configure --prefix=/usr/local && \
    make && \
    make altinstall

#RUN wget https://bootstrap.pypa.io/ez_setup.py && \
#    python2.7 ez_setup.py && \
#    easy_install pip && \
#    rm -rf Python-2.7.6.tar

RUN rm -f python && ln -s python Python-2.7.6

#RUN python get-pip.py

RUN pip install boutiques
RUN pip install simtools

ENV PATH="/usr/local/bin:$PATH"

CMD ["/etc/bootstrap.sh","-bash"]

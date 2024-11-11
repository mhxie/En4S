FROM amazon/aws-lambda-python:3.10

RUN yum update -y
RUN yum install -y python3-pip zip && yum clean all

RUN mkdir /packages
RUN echo "numpy==2.1.2" >> /packages/requirements.txt
RUN mkdir -p /packages/numpy-python-3.10/python/lib/python3.10/site-packages
RUN python3 -m pip install -r /packages/requirements.txt -t /packages/numpy-python-3.10/python/lib/python3.10/site-packages

WORKDIR /packages/numpy-python-3.10/
RUN zip -r9 /packages/numpy-python310.zip .
WORKDIR /packages/
RUN rm -rf /packages/numpy-python-3.10/
FROM amazon/aws-lambda-python:3.10

RUN yum update -y
RUN yum install -y python3-pip zip gcc python3-devel && yum clean all

RUN mkdir /packages
RUN echo "setuptools" > /packages/requirements.txt
RUN echo "thrift==0.20.0" >> /packages/requirements.txt
RUN mkdir -p /packages/thrift-python-3.10/python/lib/python3.10/site-packages

# Install setuptools and thrift
RUN python3 -m pip install -r /packages/requirements.txt -t /packages/thrift-python-3.10/python/lib/python3.10/site-packages

WORKDIR /packages/thrift-python-3.10/
RUN zip -r9 /packages/thrift-python310.zip .
WORKDIR /packages/
RUN rm -rf /packages/thrift-python-3.10/
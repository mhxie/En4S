FROM amazon/aws-lambda-python:3.10

RUN yum update -y
RUN yum install -y python3-pip zip && yum clean all

RUN mkdir /packages
RUN echo "imageio" >> /packages/requirements.txt
RUN echo "imageio[ffmpeg]" >> /packages/requirements.txt
RUN echo "imageio[opencv]" >> /packages/requirements.txt
RUN mkdir -p /packages/imageio-python-3.10/python/lib/python3.10/site-packages
RUN python3 -m pip install -r /packages/requirements.txt -t /packages/imageio-python-3.10/python/lib/python3.10/site-packages

WORKDIR /packages/imageio-python-3.10/
RUN zip -r9 /packages/iio-python310.zip .
WORKDIR /packages/
RUN rm -rf /packages/imageio-python-3.10/

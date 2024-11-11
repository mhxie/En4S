FROM amazon/aws-lambda-python:3.10

RUN yum update -y
RUN yum install -y python3-pip zip && yum clean all

RUN mkdir /packages
RUN echo "numpy==1.21.6" >> /packages/requirements.txt
RUN echo "opencv-python-headless==4.5.5.64" >> /packages/requirements.txt
# RUN pip uninstall numpy
# RUN pip install numpy
RUN mkdir -p /packages/cv2-python-3.10/python/lib/python3.10/site-packages
RUN python3 -m pip install -r /packages/requirements.txt -t /packages/cv2-python-3.10/python/lib/python3.10/site-packages


WORKDIR /packages/cv2-python-3.10/
RUN zip -r9 /packages/cv2-python310.zip .
WORKDIR /packages/
RUN rm -rf /packages/cv2-python-3.10/

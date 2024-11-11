FROM amazon/aws-lambda-python:3.10

RUN yum update -y && \
    yum install -y python3 python3-pip zip && \
    yum clean all

WORKDIR /build

COPY ../app/analytics /build/app/analytics
RUN rm -rf /build/app/analytics/__pycache__
RUN rm -rf /build/app/analytics/test*
COPY ../build /build/lib
COPY ../app/asynces.py /build/lib

RUN pip3 install -r /build/app/analytics/requirements.txt -t /build/package
RUN cp -r /build/app/analytics/* /build/package/
RUN cp -r /build/lib/* /build/package/

WORKDIR /build/package

RUN zip -r9 /build/function.zip .
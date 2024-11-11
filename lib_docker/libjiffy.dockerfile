FROM ubuntu:20.04

WORKDIR /
RUN apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install \ 
  -y -qq --no-install-recommends build-essential cmake libboost-all-dev maven \
  python3 python3-pip python3-setuptools git zip wget zlib1g-dev libssl-dev \
  libcurl4-openssl-dev libtool autoconf automake libnuma-dev
RUN git clone https://github.com/resource-disaggregation/jiffy.git
RUN cd /jiffy && mkdir build \
  && cd build \
  && cmake -DBUILD_TESTS=OFF -DBUILD_JAVA_CLIENT=OFF .. \
  && make -j8 install

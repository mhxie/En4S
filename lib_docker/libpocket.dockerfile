FROM ubuntu:20.04

ENV PYTHON_INC_OLD='/usr/include/python3.5'
ENV PYTHON_INC_NEW='/usr/include/python3.8'
ENV CMakeFile_PATH='/pocket/client/pocket/CMakeLists.txt'

WORKDIR /
RUN apt-get update -y && DEBIAN_FRONTEND=noninteractive apt-get install -y \
  build-essential libboost-all-dev cmake python3-dev git
RUN git clone https://github.com/stanford-mast/pocket.git
RUN sed -i "s/1.58/1.71/g" $CMakeFile_PATH
RUN sed -i "s/COMPONENTS python/COMPONENTS python38/g" $CMakeFile_PATH
RUN sed -i "s%$PYTHON_INC_OLD%$PYTHON_INC_NEW%g" $CMakeFile_PATH
RUN cd /pocket/client && mkdir build && cd build && cmake .. && make

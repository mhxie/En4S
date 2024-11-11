#!/bin/bash

git clone https://github.com/spdk/spdk.git spdk && \
    cd spdk && \
    git checkout 1a527e501f810e2b39b9862c96f3e8bdc465db80 && \
    git submodule update --init --recursive && \
    cd ..

git clone https://github.com/ix-project/pcidma.git pcidma && \
    cd pcidma && \
    git checkout 968694f1e21248ba70d39a5432e1cbeafc73dbd5 && \
    cd ..

git clone https://github.com/lwip-tcpip/lwip.git lwip && \
    cd lwip && \
    git checkout 347054b329d141b858cc7180bf2aa23c672dc6d7 && \
    cd ..
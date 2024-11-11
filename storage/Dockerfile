FROM ubuntu:20.04 AS base

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y net-tools libpciaccess-dev
# RUN apt install linux-headers-$(uname -r)

FROM base AS prebuild

RUN apt-get install -y git libconfig-dev
COPY . /en4s_storage

WORKDIR /en4s_storage

# Clone each submodule and checkout the specific commit
RUN git clone https://github.com/spdk/spdk.git spdk && \
    cd spdk && \
    git checkout 1a527e501f810e2b39b9862c96f3e8bdc465db80 && \
    git submodule update --init --recursive

RUN git clone https://github.com/ix-project/pcidma.git pcidma && \
    cd pcidma && \
    git checkout 968694f1e21248ba70d39a5432e1cbeafc73dbd5

RUN git clone https://github.com/lwip-tcpip/lwip.git lwip && \
    cd lwip && \
    git checkout 347054b329d141b858cc7180bf2aa23c672dc6d7

# Continue with the rest of the build process
RUN ./spdk/scripts/pkgdep.sh
RUN sed -i 's|mempool/ring|mempool/ring net/ena|g' spdk/dpdkbuild/Makefile
RUN sed -i 's|false|true|g' spdk/dpdk/lib/librte_timer/meson.build
RUN sed -i 's|#include "lwip/memp.h"|#include "lwip_if/memp.h"|' lwip/src/include/lwip/stats.h
RUN cp usertools/openloop_perf/* spdk/examples/nvme/perf/

FROM prebuild as build

# Uncomment the following line in your AWS EC2 instance
# RUN cd spdk && ./configure --with-igb-uio-driver && make && cd ..
# Comment the following line in your AWS EC2 instance
RUN cd spdk && ./configure && make && cd ..
RUN meson build && meson compile -C build

FROM prebuild

# Copy binary
COPY --from=build /en4s_storage/build/dp /home/dp
# Copy dependencies
COPY --from=build /en4s_storage/spdk/build/lib /home/spdk/build/lib
COPY --from=build /en4s_storage/spdk/dpdk/build/lib /home/spdk/dpdk/build/lib
# Uncomment the following line in your AWS EC2 instance
# COPY --from=build /en4s_storage/spdk/dpdk/build-tmp/kernel/linux/igb_uio/igb_uio.ko /home/en4s_storage/spdk/dpdk
# Copy configurations
COPY --from=build /en4s_storage/ix.conf.sample /home
COPY --from=build /en4s_storage/sample-aws-ec2.devmodel /home
# Copy startup script
COPY --from=build /en4s_storage/spdk/scripts/setup.sh /home/spdk
COPY --from=build /en4s_storage/spdk/dpdk/usertools/dpdk-devbind.py /home
COPY --from=build /en4s_storage/usertools/conf_setup.sh /home
COPY --from=build /en4s_storage/usertools/start.sh /home

# WORKDIR /home
# CMD ["./start.sh"]
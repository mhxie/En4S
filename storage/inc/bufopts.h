#ifndef BUFOPTS_H
#define BUFOPTS_H

#define SERVER_MEMP_SZ 4 * 1024
#define CLIENT_MEMP_SZ 1024
//#define SERVER_NVME_MEMP_SZ  512 * 1024
#define SERVER_NVME_MEMP_SZ  128 * 4096
#define IXEV_MEMP_SZ 16 * 1024
//#define NVME_MEMP_SZ 512 * 1024
#define NVME_MEMP_SZ 128 * 4096
/* Capacity should be at least RX queues per CPU * ETH_DEV_RX_QUEUE_SZ */
#define MBUF_CAPACITY 16 * 1024

#endif
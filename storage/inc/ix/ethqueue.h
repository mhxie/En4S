/*
 * Copyright (c) 2015-2017, Stanford University
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 *  * Neither the name of the copyright holder nor the names of its
 *    contributors may be used to endorse or promote products derived from
 *    this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * Copyright 2013-16 Board of Trustees of Stanford University
 * Copyright 2013-16 Ecole Polytechnique Federale Lausanne (EPFL)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

/*
 * ethqueue.h - ethernet queue support
 */

#pragma once

#include <ix/bitmap.h>
#include <ix/cpu.h>
#include <ix/errno.h>
// #include <ix/ethfg.h>
#include <ix/mbuf.h>
#include <lwip/netif.h>
#include <rte_ethdev.h>
#include <rte_per_lcore.h>

#define ETH_DEV_RX_QUEUE_SZ 512
#define ETH_DEV_TX_QUEUE_SZ 1024  // 4096
#define ETH_RX_MAX_DEPTH 32768
#define MAX_NUM_NETIFS 4

extern unsigned int eth_rx_max_batch;
extern struct netif g_netif[MAX_NUM_NETIFS];

/*
 * Recieve Queue API
 */

struct eth_rx_queue {
    void *perqueue_offset;

    struct mbuf *head; /* pointer to first recieved buffer */
    struct mbuf *tail; /* pointer to last recieved buffer */
    int len;           /* the total number of buffers */
    int queue_idx;     /* the queue index number */

    /* poll for new packets */
    int (*poll)(struct eth_rx_queue *rx);

    // /* a bitmap of flow groups directed to this queue */
    // DEFINE_BITMAP(assigned_fgs, ETH_MAX_NUM_FG);

    // struct ix_rte_eth_dev *dev;
    struct rte_eth_dev *dev;
};

RTE_DECLARE_PER_LCORE(struct rte_eth_dev_tx_buffer *, tx_buf);

/**
 * ethdev_init_cpu - initializes the core-local tx buffer
 *
 * Returns 0 if successful, otherwise failure.
 */

int ethdev_init_cpu(void);
err_t dpdk_netif_init(struct netif *netif);

/**
 * eth_rx_poll - recieve pending packets on an RX queue
 * @rx: the RX queue
 *
 * Returns the number of packets recieved.
 */
static inline int eth_rx_poll(struct eth_rx_queue *rx) { return rx->poll(rx); }

/**
 * eth_recv - enqueues a received packet
 * @rxq: the receive queue
 * @mbuf: the packet
 *
 * Typically called by the device driver's poll() routine.
 *
 * Returns 0 if successful, otherwise the packet should be dropped.
 */
static inline int eth_recv(struct eth_rx_queue *rxq, struct mbuf *mbuf) {
    // TODO: Need to figure out why fg_transition is dropping packet
    // fg->cur_cpu is not being set properly. Probably something with
    // initialization...
    if (eth_recv_handle_fg_transition(rxq, mbuf)) return 0;

    if (unlikely(rxq->len >= ETH_RX_MAX_DEPTH)) return -EBUSY;

    mbuf->next = NULL;

    if (!rxq->head) {
        rxq->head = mbuf;
        rxq->tail = mbuf;
    } else {
        rxq->tail->next = mbuf;
        rxq->tail = mbuf;
    }

    rxq->len++;
    return 0;
}

extern bool eth_rx_idle_wait(uint64_t max_usecs);
extern bool eth_rx_check(void);

/*
 * Transmit Queue API
 */

struct eth_tx_queue {
    int cap; /* number of available buffers left */
    int len; /* number of buffers used so far */
    struct mbuf *bufs[ETH_DEV_TX_QUEUE_SZ];

    int (*reclaim)(struct eth_tx_queue *tx);
    int (*xmit)(struct eth_tx_queue *tx, int nr, struct mbuf **mbufs);
};

/**
 * eth_tx_reclaim - scans the queue and reclaims finished buffers
 * @tx: the TX queue
 *
 * NOTE: scatter-gather mbuf's can span multiple descriptors, so
 * take that into account when interpreting the count provided by
 * this function.
 *
 * Returns an available descriptor count.
 */
static inline int eth_tx_reclaim(struct eth_tx_queue *tx) {
    return tx->reclaim(tx);
}

/**
 * eth_tx_xmit - transmits packets on a TX queue
 * @tx: the TX queue
 * @nr: the number of mbufs to transmit
 * @mbufs: an array of mbufs to process
 *
 * Returns the number of mbuf's transmitted.
 */
static inline int eth_tx_xmit(struct eth_tx_queue *tx, int nr,
                              struct mbuf **mbufs) {
    return tx->xmit(tx, nr, mbufs);
}

/* FIXME: convert to per-flowgroup */
// DECLARE_PERQUEUE(struct eth_tx_queue *, eth_txq);

/**
 * eth_send - enqueues a packet to be sent
 * @mbuf: the packet
 *
 * Returns 0 if successful, otherwise out of space.
 */
static inline int eth_send(struct eth_tx_queue *txq, struct mbuf *mbuf) {
    int nr = 1 + mbuf->nr_iov;
    if (unlikely(nr > txq->cap)) return -EBUSY;

    txq->bufs[txq->len++] = mbuf;
    txq->cap -= nr;

    return 0;
}

/**
 * eth_send_one - enqueues a packet without scatter-gather to be sent
 * @mbuf: the packet
 * @len: the length of the packet
 *
 * Returns 0 if successful, otherwise out of space.
 */
static inline int eth_send_one(struct eth_tx_queue *txq, struct mbuf *mbuf,
                               size_t len) {
    mbuf->dpdk_mbuf->pkt_len = len;
    mbuf->dpdk_mbuf->data_len = len;

    mbuf->nr_iov = 0;

    return eth_send(txq, mbuf);
}

RTE_DECLARE_PER_LCORE(int, eth_num_queues);
RTE_DECLARE_PER_LCORE(struct eth_rx_queue *, eth_rxqs[]);
RTE_DECLARE_PER_LCORE(struct eth_tx_queue *, eth_txqs[]);

extern int eth_process_poll(void);
extern int eth_process_recv(void);
extern void eth_process_send(void);
extern void eth_process_reclaim(void);

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
 * ethqueue.c - ethernet queue support
 */

#include <ix/byteorder.h>
// #include <ix/control_plane.h>
#include <ix/ethdev.h>
#include <ix/kstats.h>
#include <ix/log.h>
#include <ix/stddef.h>
#include <lwip/etharp.h>
#include <lwip/prot/tcp.h>
#include <rte_per_lcore.h>
#include <rte_timer.h>

/* Accumulate metrics period (in us) */
#define METRICS_PERIOD_US 10000

/* Power measurement period (in us) */
#define POWER_PERIOD_US 500000

#define MAX_PKT_BURST 64

#define EMA_SMOOTH_FACTOR_0 0.5
#define EMA_SMOOTH_FACTOR_1 0.25
#define EMA_SMOOTH_FACTOR_2 0.125
#define EMA_SMOOTH_FACTOR EMA_SMOOTH_FACTOR_0
// #define MAX_NUM_IO_QUEUES 31
// #define MAX_NUM_IO_QUEUES 130
#define MAX_NUM_IO_QUEUES 127
#define REFACTORING 1

struct netif g_netif[MAX_NUM_NETIFS];

RTE_DEFINE_PER_LCORE(int, eth_num_queues);
RTE_DEFINE_PER_LCORE(struct eth_rx_queue *, eth_rxqs[NETHDEV]);
RTE_DEFINE_PER_LCORE(struct eth_tx_queue *, eth_txqs[NETHDEV]);

struct metrics_accumulator {
    long timestamp;
    long queuing_delay;
    int batch_size;
    int count;
    long queue_size;
    long loop_duration;
    long prv_timestamp;
};

typedef struct my_custom_pbuf {
    struct pbuf_custom p;
    struct rte_mbuf *m;
} my_custom_pbuf_t;

static RTE_DEFINE_PER_LCORE(struct metrics_accumulator, metrics_acc);

struct power_accumulator {
    int prv_energy;
    long prv_timestamp;
};

static struct power_accumulator power_acc;

// uint64_t cycles_per_us = rte_get_timer_hz() / 1E6;
unsigned int eth_rx_max_batch = 64;  // 128; //64

// #define PRINT_RTE_STATS 1

#ifdef PRINT_RTE_STATS
static unsigned long count_stats = 0;
#endif

err_t pbuf_to_mbuf(struct pbuf *p, struct rte_mbuf **m) {
    u16_t tot_len = 0;
    char *data_ptr = NULL;
    if (p == NULL) {
        return ERR_ARG;  // Invalid argument
    }
    // Allocate an mbuf from the global mempool
    *m = mbuf_alloc_local();
    if (*m == NULL) {
        return ERR_MEM;  // Out of memory error
    }

#if LWIP_NETIF_TX_SINGLE_PBUF
    assert(p->next == NULL);
    data_ptr = rte_pktmbuf_mtod(*m, char *);
#else
    while (p) {
        data_ptr = rte_pktmbuf_mtod_offset(*m, char *, tot_len);
#endif
    tot_len += p->tot_len;
    // Ensure the mbuf has enough space for the pbuf data
    if (rte_pktmbuf_tailroom(*m) < p->tot_len) {
        printf("pbuf_to_mbuf: not enough tailroom\n");
        rte_pktmbuf_free(*m);
        return ERR_BUF;  // Buffer error
    }

    // memcpy(data_ptr, current_pbuf->payload, current_pbuf->len);
    if (pbuf_copy_partial(p, data_ptr, p->len, 0) != p->len) {
        printf("pbuf_to_mbuf: pbuf_copy_partial failed\n");
        return ERR_MEM;
    }
    // Update the data length in the mbuf

    rte_pktmbuf_data_len(*m) = tot_len;
    rte_pktmbuf_pkt_len(*m) = tot_len;

#if !LWIP_NETIF_TX_SINGLE_PBUF
}
#endif

return ERR_OK;  // Success
}

void pbuf_free_cb(void *p, void *arg) { pbuf_free(p); }

err_t pbuf_to_mbuf_zc(struct pbuf *p, struct rte_mbuf **m) {
    static struct rte_mbuf_ext_shared_info shinfo;

    if (p == NULL) {
        return ERR_ARG;  // Invalid argument
    }

    assert(p->next == NULL);  // Currently, only supports non-chained pbufs

    // Allocate an mbuf from the global mempool
    *m = mbuf_alloc_local();
    if (*m == NULL) {
        return ERR_MEM;  // Out of memory error
    }

    rte_mbuf_ext_refcnt_set(&shinfo, 1);
    shinfo.free_cb = pbuf_free_cb;

    pbuf_ref(p);  // Avoid freeing the pbuf before the mbuf is freed
    // FIXME: find a way to get rte_iova_t
    rte_pktmbuf_attach_extbuf(*m, p->payload, NULL, p->tot_len, &shinfo);

    // Set data length in the mbuf
    rte_pktmbuf_data_len(*m) = p->tot_len;
    rte_pktmbuf_pkt_len(*m) = p->tot_len;

    return ERR_OK;  // Success
}

err_t mbuf_to_pbuf(struct pbuf **p, struct rte_mbuf *m) {
    if (!m) {
        return ERR_ARG;  // Invalid argument
    }

    // Allocate a pbuf of the appropriate size. Use PBUF_RAW for raw bytes.
    *p = pbuf_alloc(PBUF_RAW, rte_pktmbuf_data_len(m), PBUF_POOL);
    if (*p == NULL) {
        return ERR_MEM;  // Failed to allocate pbuf
    }

    // Copy the data from the mbuf to the pbuf
    if (rte_pktmbuf_data_len(m) > (*p)->len) {
        pbuf_free(*p);   // Ensure to free allocated pbuf on failure
        return ERR_MEM;  // This should ideally be a different error, indicating
                         // size mismatch
    }

    // Use memcpy to copy the data. rte_pktmbuf_mtod() gets the data pointer.
    memcpy((*p)->payload, rte_pktmbuf_mtod(m, void *), rte_pktmbuf_data_len(m));
    rte_pktmbuf_free(m);

    return ERR_OK;  // Success
}

void my_pbuf_free(void *p) {
    my_custom_pbuf_t *my_puf = (my_custom_pbuf_t *)p;

    rte_pktmbuf_free(my_puf->m);
    free(my_puf);
}

err_t mbuf_to_pbuf_zc(struct pbuf **pp, struct rte_mbuf *m) {
    if (m == NULL) return ERR_ARG;
    my_custom_pbuf_t *pc = malloc(sizeof(struct pbuf_custom));
    pc->p.custom_free_function = my_pbuf_free;
    pc->m = m;

    // Calculate the total data length in the mbuf chain
    uint16_t total_len = rte_pktmbuf_pkt_len(m);

    // Allocate a custom pbuf that wraps the mbuf data
    *pp = pbuf_alloced_custom(PBUF_RAW, total_len, PBUF_REF, &pc->p,
                              rte_pktmbuf_mtod(m, void *), m->buf_len);
    if (*pp == NULL) {
        pbuf_free(pp);
        printf("mbuf_to_pbuf_zc: pbuf_alloced_custom failed\n");
        return ERR_MEM;
    }

    return ERR_OK;
}

/**
 * eth_process_poll - polls HW for new packets
 *
 * Returns the number of new packets received.
 */
int eth_process_poll(void) {
    int i, ret = 0;
    int count = 0;
    bool empty;
    struct rte_mbuf *m;
    struct pbuf *p;
    int eff_len = 0;

    struct rte_mbuf
        *rx_pkts[MAX_NUM_IO_QUEUES];  // TODO: test with multi queue, multicore;
                                      // assumes 1 pkt recv at a time

    /*
     * We round robin through each queue one packet at
     * a time for fairness, and stop when all queues are
     * empty or the batch limit is hit. We're okay with
     * going a little over the batch limit if it means
     * we're not favoring one queue over another.
     */
    // log_info("Start looping:\n");
    do {
        empty = true;

        // This loops over each queue of a single core (current behaviour is 1
        // queue to 1 core) Note that i is the index within a core's list of
        // queues, not the global list of queues. log_info("Processing %d
        // queues...\n", percpu_get(eth_num_queues));
        for (i = 0; i < percpu_get(eth_num_queues); i++) {
            // burst 1 because check queues round-robin
            //  Note: Using percpu_get(cpu_id) requires one queue to one core
            //  and identical cpu and queue numbering.
            // log_info("Now the g_active_eth_port is %d\n", g_active_eth_port);
            ret = rte_eth_rx_burst(g_active_eth_port, percpu_get(cpu_id),
                                   &rx_pkts[i], 1);
            if (ret && i > MAX_NUM_IO_QUEUES) printf("Out of boundary.\n");
            if (ret) {
#ifdef MQ_DEBUG
                printf("One packet at queue %d.\n", percpu_get(cpu_id));
#endif
                empty = false;
                m = rx_pkts[i];
                eff_len = m->pkt_len - 54;
                if (eff_len > 24) {
                    // printf("eth_process_poll: len %d\n", eff_len);
                } else {
                    // printf("eth_process_poll: smaller than 24.\n");
                }

                rte_prefetch0(rte_pktmbuf_mtod(m, void *));
                LINK_STATS_INC(link.recv);
                // eth_input_process(rx_pkts[i], ret);
#ifdef TCP_INPUT_DEBUG
                if (m->ol_flags & PKT_RX_TIMESTAMP) {
                    printf("A new packet arrived at %" PRIu64 ".\n",
                           m->timestamp);
                }
                if (unlikely(m->ol_flags & PKT_RX_IP_CKSUM_MASK) ==
                    PKT_RX_IP_CKSUM_BAD)
                    printf("Offloaded IP chksum corrupted.\n");

                if (unlikely(m->ol_flags & PKT_RX_L4_CKSUM_MASK) ==
                    PKT_RX_L4_CKSUM_BAD)
                    printf("Offloaded TCP chksum corrupted.\n");
#endif
                // convert rte_buf to pbuf
                ret = mbuf_to_pbuf_zc(&p, m);
                if (ret == ERR_MEM) {
                    printf("mbuf_to_pbuf failed\n");
                    rte_pktmbuf_free(m);
                    LINK_STATS_INC(link.drop);
                    continue;
                }
                // FIXME: It is recommended to use netif->input() for packet
                // processing
                if (ethernet_input(p, &g_netif[g_active_eth_port]) != ERR_OK) {
                    printf("ethernet_input failed\n");
                    rte_pktmbuf_free(m);
                    LINK_STATS_INC(link.drop);
                    continue;
                }
                // if(netif->input(p, netif) != ERR_OK) {
                //     pbuf_free(p);
                // }

#ifdef PRINT_RTE_STATS
                struct rte_eth_stats stats;
                rte_eth_stats_get(0, &stats);
                count_stats++;
                if (count_stats % 1000000 == 0) {
                    printf(
                        "Stats: NIC drop or error:  %f out of %f \n",
                        (double)stats.imissed + stats.ierrors + stats.oerrors,
                        (double)(stats.imissed + stats.ipackets));
                }
#endif
            }
            count += ret;
        }
    } while (!empty && count < eth_rx_max_batch);

    // log_info("End looping.\n");s
    return count;
}

#ifndef REFACTORING
static int eth_process_recv_queue(struct eth_rx_queue *rxq) {
    struct mbuf *pos = rxq->head;
#ifdef ENABLE_KSTATS
    kstats_accumulate tmp;
#endif

    if (!pos) return -EAGAIN;

    /* NOTE: pos could get freed after eth_input(), so check next here */
    rxq->head = pos->next;
    rxq->len--;

    KSTATS_PUSH(eth_input, &tmp);
    eth_input(rxq, pos);
    KSTATS_POP(&tmp);

    return 0;
}

/**
 * eth_process_recv - processes pending received packets, not in use
 *
 * Returns true if there are no remaining packets.
 */
int eth_process_recv(void) {
    int i, count = 0;
    bool empty;
    unsigned long min_timestamp = -1;
    unsigned long timestamp;
    int value;
    struct metrics_accumulator *this_metrics_acc = &percpu_get(metrics_acc);
    int backlog;
    double idle;
    unsigned int energy;
    int energy_diff;

    /*
     * We round robin through each queue one packet at
     * a time for fairness, and stop when all queues are
     * empty or the batch limit is hit. We're okay with
     * going a little over the batch limit if it means
     * we're not favoring one queue over another.
     */
    do {
        empty = true;
        for (i = 0; i < percpu_get(eth_num_queues); i++) {
            struct eth_rx_queue *rxq = percpu_get(eth_rxqs[i]);
            struct mbuf *pos = rxq->head;
            if (pos) min_timestamp = min(min_timestamp, pos->timestamp);
            if (!eth_process_recv_queue(rxq)) {
                count++;
                empty = false;
            }
        }
    } while (!empty && count < eth_rx_max_batch);

    backlog = 0;
    for (i = 0; i < percpu_get(eth_num_queues); i++)
        backlog += percpu_get(eth_rxqs[i])->len;

    timestamp = rdtsc();
    this_metrics_acc->count++;
    value = count ? (timestamp - min_timestamp) / cycles_per_us : 0;
    this_metrics_acc->queuing_delay += value;
    this_metrics_acc->batch_size += count;
    this_metrics_acc->queue_size += count + backlog;
    this_metrics_acc->loop_duration +=
        timestamp - this_metrics_acc->prv_timestamp;
    this_metrics_acc->prv_timestamp = timestamp;
    if (timestamp - this_metrics_acc->timestamp >
        (long)cycles_per_us * METRICS_PERIOD_US) {
        idle = (double)percpu_get(idle_cycles) /
               (timestamp - this_metrics_acc->timestamp);
        EMA_UPDATE(cp_shmem->cpu_metrics[RTE_PER_LCORE(cpu_nr)].idle[0], idle,
                   EMA_SMOOTH_FACTOR_0);
        EMA_UPDATE(cp_shmem->cpu_metrics[RTE_PER_LCORE(cpu_nr)].idle[1], idle,
                   EMA_SMOOTH_FACTOR_1);
        EMA_UPDATE(cp_shmem->cpu_metrics[RTE_PER_LCORE(cpu_nr)].idle[2], idle,
                   EMA_SMOOTH_FACTOR_2);
        if (this_metrics_acc->count) {
            this_metrics_acc->loop_duration -= percpu_get(idle_cycles);
            this_metrics_acc->loop_duration /= cycles_per_us;
            EMA_UPDATE(
                cp_shmem->cpu_metrics[RTE_PER_LCORE(cpu_nr)].queuing_delay,
                (double)this_metrics_acc->queuing_delay /
                    this_metrics_acc->count,
                EMA_SMOOTH_FACTOR);
            EMA_UPDATE(
                cp_shmem->cpu_metrics[RTE_PER_LCORE(cpu_nr)].batch_size,
                (double)this_metrics_acc->batch_size / this_metrics_acc->count,
                EMA_SMOOTH_FACTOR);
            EMA_UPDATE(
                cp_shmem->cpu_metrics[RTE_PER_LCORE(cpu_nr)].queue_size[0],
                (double)this_metrics_acc->queue_size / this_metrics_acc->count,
                EMA_SMOOTH_FACTOR_0);
            EMA_UPDATE(
                cp_shmem->cpu_metrics[RTE_PER_LCORE(cpu_nr)].queue_size[1],
                (double)this_metrics_acc->queue_size / this_metrics_acc->count,
                EMA_SMOOTH_FACTOR_1);
            EMA_UPDATE(
                cp_shmem->cpu_metrics[RTE_PER_LCORE(cpu_nr)].queue_size[2],
                (double)this_metrics_acc->queue_size / this_metrics_acc->count,
                EMA_SMOOTH_FACTOR_2);
            EMA_UPDATE(
                cp_shmem->cpu_metrics[RTE_PER_LCORE(cpu_nr)].loop_duration,
                (double)this_metrics_acc->loop_duration /
                    this_metrics_acc->count,
                EMA_SMOOTH_FACTOR_0);
        } else {
            EMA_UPDATE(
                cp_shmem->cpu_metrics[RTE_PER_LCORE(cpu_nr)].queuing_delay, 0,
                EMA_SMOOTH_FACTOR);
            EMA_UPDATE(cp_shmem->cpu_metrics[RTE_PER_LCORE(cpu_nr)].batch_size,
                       0, EMA_SMOOTH_FACTOR);
            EMA_UPDATE(
                cp_shmem->cpu_metrics[RTE_PER_LCORE(cpu_nr)].queue_size[0], 0,
                EMA_SMOOTH_FACTOR_0);
            EMA_UPDATE(
                cp_shmem->cpu_metrics[RTE_PER_LCORE(cpu_nr)].queue_size[1], 0,
                EMA_SMOOTH_FACTOR_1);
            EMA_UPDATE(
                cp_shmem->cpu_metrics[RTE_PER_LCORE(cpu_nr)].queue_size[2], 0,
                EMA_SMOOTH_FACTOR_2);
            EMA_UPDATE(
                cp_shmem->cpu_metrics[RTE_PER_LCORE(cpu_nr)].loop_duration, 0,
                EMA_SMOOTH_FACTOR_0);
        }
        this_metrics_acc->timestamp = timestamp;
        percpu_get(idle_cycles) = 0;
        this_metrics_acc->count = 0;
        this_metrics_acc->queuing_delay = 0;
        this_metrics_acc->batch_size = 0;
        this_metrics_acc->queue_size = 0;
        this_metrics_acc->loop_duration = 0;
    }
    // NOTE: assuming that the first CPU never idles
    /* TODO: NOT SUPPORTING ENERGY CHANGES RIGHT NOW
        if (percpu_get(cpu_nr) == 0 && timestamp - power_acc.prv_timestamp >
       (long) cycles_per_us * POWER_PERIOD_US) { energy =
       rdmsr(MSR_PKG_ENERGY_STATUS); if (power_acc.prv_timestamp) { energy_diff
       = energy - power_acc.prv_energy; if (energy_diff < 0) energy_diff += 1 <<
       31; cp_shmem->pkg_power = (double) energy_diff * energy_unit / (timestamp
       - power_acc.prv_timestamp) * cycles_per_us * 1000000; } else {
                        cp_shmem->pkg_power = 0;
                }
                power_acc.prv_timestamp = timestamp;
                power_acc.prv_energy = energy;
        }
        */

    KSTATS_PACKETS_INC(count);
    KSTATS_BATCH_INC(count);
#ifdef ENABLE_KSTATS
    backlog = div_up(backlog, eth_rx_max_batch);
    KSTATS_BACKLOG_INC(backlog);
#endif

    return empty;
}
#endif

RTE_DEFINE_PER_LCORE(struct rte_eth_dev_tx_buffer *, tx_buf);

/*
 * Tx buffer error callback
 */
static void flush_tx_error_callback(struct rte_mbuf **unsent, uint16_t count,
                                    void *userdata) {
    int i;
    // uint16_t port_id = (uintptr_t)userdata;
    // tx_stats->tx_drop[port_id] += count;
    printf("TX flush error.\n");
    /* free the mbufs which failed from transmit */
    for (i = 0; i < count; i++) {
        rte_pktmbuf_free(unsent[i]);
        LINK_STATS_INC(link.drop);
    }
}

static err_t dpdk_output(struct netif *netif, struct pbuf *p) {
    int ret;

#if LWIP_NETIF_TX_SINGLE_PBUF
    while (p) {
#endif
        struct rte_mbuf *pkt;
        struct rte_ether_hdr *ethhdr;
        struct ip_hdr *iphdr;
        struct tcp_hdr *tcphdr;
        ret = pbuf_to_mbuf(p, &pkt);
        // ret = pbuf_to_mbuf_zc(p, &pkt); // FIXME: this is not working

        if (ret == ERR_MEM) {
            printf("dpdk_output: pbuf_to_rte_mbuf allocation failed\n");
            return ERR_MEM;
        }
        ethhdr = rte_pktmbuf_mtod(pkt, struct rte_ether_hdr *);
        if (likely(ethhdr->ether_type ==
                   rte_be_to_cpu_16(RTE_ETHER_TYPE_IPV4))) {
            /* Offload IP and TCP tx checksums */
#if !CHECKSUM_GEN_IP && !CHECKSUM_GEN_TCP
            pkt->ol_flags = PKT_TX_IP_CKSUM;
            pkt->ol_flags |= PKT_TX_TCP_CKSUM;
            // pkt->ol_flags |= PKT_TX_IPV4;
            pkt->l2_len = sizeof(struct eth_hdr);
            pkt->l3_len = sizeof(struct ip_hdr);
            iphdr = mbuf_nextd(ethhdr, struct ip_hdr *);
            tcphdr = mbuf_nextd(iphdr, struct tcp_hdr *);
            LWIP_ASSERT("ip checksum is 0", ipv4_hdr->hdr_checksum == 0);
            tcphdr->chksum = rte_ipv4_phdr_cksum(iphdr, pkt->ol_flags);
#endif
        }

        // ret = rte_eth_tx_buffer(g_active_eth_port, percpu_get(cpu_id),
        //                         percpu_get(tx_buf), pkt);

        // if we want zero-copy we need to finish the tx burst first
        ret = rte_eth_tx_burst(g_active_eth_port, percpu_get(cpu_id), &pkt, 1);

        if (unlikely(ret < 0)) {
            printf("dpdk_output error: tx ret is %d\n", ret);
            rte_pktmbuf_free(pkt);
            LINK_STATS_INC(link.drop);
            return ERR_IF;
        }
        LINK_STATS_INC(link.xmit);
#if LWIP_NETIF_TX_SINGLE_PBUF
        p = p->next;
        LWIP_ASSERT("dpdk_output: pbuf chain not supported\n", p == NULL);
    }
#endif

    // pbuf_free(p); // Not needed since TCP will handle the result

    return ERR_OK;
}

static void dpdk_input(struct rte_mbuf *pkt, struct netif *netif) {
    // raise not implemented error
    printf("dpdk_input not implemented\n");
}

err_t ethif_init(struct netif *netif) {
    // FIXME: only supports one port
    netif->name[0] = 'e';
    netif->name[1] = '0' + g_active_eth_port;
    // netif->input = dpdk_input;
    netif->output = etharp_output;
    netif->linkoutput = dpdk_output;
    // Set additional netif fields such as MTU, flags, etc.

    // // Optionally set up a hardware address (MAC)
    // FIXME: we enabled LWIP_NETIF_HWADDRHINT
    // see NETIF_SET_HINTS(netif, &(pcb->netif_hints)) @tcp_out.c
    // netif->hints = NULL;
    // memcpy(netif->hwaddr, my_mac_address, ETH_HWADDR_LEN);

    // Set NETIF_FLAG_LINK_UP if the link is up at initialization,
    // otherwise, this should be set by your link status monitoring mechanism
    netif->flags |= NETIF_FLAG_UP | NETIF_FLAG_BROADCAST | NETIF_FLAG_ETHARP |
                    NETIF_FLAG_LINK_UP;
    netif->hwaddr_len = ETH_HWADDR_LEN;
    return ERR_OK;
}

// Network interface initialization function
err_t dpdk_netif_init(struct netif *netif) {
    // add the new netif to the list of network interfaces
    netif_add_noaddr(netif, NULL, ethif_init, dpdk_input);

    return ERR_OK;
}

/**
 * ethdev_init_cpu - initializes the core-local tx buffer
 *
 * Returns 0 if successful, otherwise failure.
 */

int ethdev_init_cpu(void) {
    int ret;
    struct rte_eth_dev_tx_buffer *tx_buffer = percpu_get(tx_buf);

    tx_buffer = rte_zmalloc_socket("tx_buffer",
                                   RTE_ETH_TX_BUFFER_SIZE(eth_rx_max_batch), 0,
                                   rte_eth_dev_socket_id(g_active_eth_port));
    printf("The size of RTE_ETH_TX_BUFFER_SIZE (eth_rx_max_batch) is %ld\n.",
           RTE_ETH_TX_BUFFER_SIZE(eth_rx_max_batch));
    if (tx_buffer == NULL) {
        log_err("ERROR: cannot allocate buffer for tx \n");
        exit(0);
    }

    ret = rte_eth_tx_buffer_init(tx_buffer, eth_rx_max_batch);
    if (ret) {
        log_err("ERROR in tx buffer init\n");
        exit(0);
    }
    percpu_get(tx_buf) = tx_buffer;
    printf("tx_buffer: size-%d, len-%d.\n", tx_buffer->size, tx_buffer->length);

    ret = rte_eth_tx_buffer_set_err_callback(tx_buffer, flush_tx_error_callback,
                                             NULL);
    if (ret < 0)
        rte_exit(EXIT_FAILURE, "Cannot set error callback for tx buffer.\n");
    // Assign each CPU the correct number of queues.
    percpu_get(eth_num_queues) = rte_eth_dev_count_avail();
    // log_info("Now we have %d queues for this core.\n",
    // percpu_get(eth_num_queues));

    return 0;
}

/**
 * eth_process_send - processes packets pending to be sent
 */
void eth_process_send(void) {
    int i, nr;
    struct eth_tx_queue *txq;

    for (i = 0; i < percpu_get(eth_num_queues); i++) {
        // NOTE: rte_eth_tx_buffer_flush appears to flush all queues regardless
        // of the parameter given. Currently incompatible with multiple queues
        // per CPU core due to cpu_id being queue number.
        nr = rte_eth_tx_buffer_flush(g_active_eth_port, percpu_get(cpu_id),
                                     percpu_get(tx_buf));
        while (nr-- > 0) LINK_STATS_INC(link.xmit);
    }
}

/**
 * eth_process_reclaim - processs packets that have completed sending
 */
void eth_process_reclaim(void) {
    int i;
    struct eth_tx_queue *txq;

    for (i = 0; i < percpu_get(eth_num_queues); i++) {
        txq = percpu_get(eth_txqs[i]);
        txq->cap = eth_tx_reclaim(txq);
    }
}
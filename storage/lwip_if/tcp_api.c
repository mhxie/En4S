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
 * tcp_api.c - plumbing between the TCP and userspace
 */

#include <assert.h>
#include <bufopts.h>
#include <ix/byteorder.h>
#include <ix/cfg.h>
#include <ix/errno.h>
#include <ix/ethdev.h>
#include <ix/kstats.h>
#include <ix/log.h>
#include <ix/stddef.h>
#include <ix/syscall.h>
#include <lwip/tcp.h>
#include <rte_per_lcore.h>
#include <sys/socket.h>

// int ip_send_one(ip_addr_t *dst_addr, struct rte_mbuf *pkt, size_t len);
#define REFACTORING
#define DEFAULT_PORT 8000

/* FIXME: this should be probably per queue */
static RTE_DEFINE_PER_LCORE(struct tcp_pcb_listen[CFG_MAX_PORTS], listen_ports);

static RTE_DEFINE_PER_LCORE(uint16_t, local_port);
/* FIXME: this should be more adaptive to various configurations */
#define PORTS_PER_CPU (65536 / 32)

/*
 * FIXME: LWIP and IX have different lifetime rules so we have to maintain
 * a seperate pcb. Otherwise, we'd be plagued by use-after-free problems.
 */
struct tcpapi_pcb {
    unsigned long alive; /* FIXME: this overlaps with mempool_hdr so
                          * we can tell if this pcb is allocated or not. */
    struct tcp_pcb *pcb;
    unsigned long cookie;
    struct ip_tuple *id;
    hid_t handle;
    struct pbuf *recvd;
    struct pbuf *recvd_tail;
    int queue;
    bool accepted;
};

#define TCPAPI_PCB_SIZE 64

static struct mempool_datastore pcb_api_datastore;
static struct mempool_datastore id_datastore;

static RTE_DEFINE_PER_LCORE(struct mempool,
                            pcb_api_mempool __attribute__((aligned(64))));
static RTE_DEFINE_PER_LCORE(struct tcpapi_pcb *,
                            handle2pcb_array[MEMP_NUM_TCP_PCB]);

static RTE_DEFINE_PER_LCORE(struct mempool,
                            id_mempool __attribute__((aligned(64))));

static void remove_fdir_filter(struct ip_tuple *id);

/**
 * handle_to_tcpapi - converts a handle to a PCB
 * @handle: the input handle
 *
 * Return a PCB, or NULL if the handle is invalid.
 */
static inline struct tcpapi_pcb *handle_to_tcpapi(hid_t handle) {
    // struct mempool *p;
    struct tcpapi_pcb *api;
    unsigned long idx = (handle & 0xffffffffffff);

    if (unlikely(idx >= MEMP_NUM_TCP_PCB)) return NULL;

    // p = &percpu_get(pcb_api_mempool);

    api = (struct tcpapi_pcb *)percpu_get(handle2pcb_array[idx]);

    // MEMPOOL_SANITY_ACCESS(api);

    /* check if the handle is actually allocated */
    // TODO: Not sure how to do this with mempools
    // if (unlikely(api->alive > 1))
    //	return NULL;

    percpu_get(syscall_cookie) = api->cookie;

    return api;
}

/**
 * tcpapi_to_handle - converts a PCB to a handle
 * @pcb: the PCB.
 *
 * Returns a handle.
 */
static inline hid_t tcpapi_to_handle(struct tcpapi_pcb *pcb) {
    struct mempool *p = &percpu_get(pcb_api_mempool);
    // MEMPOOL_SANITY_ACCESS(api);

    unsigned int i = 0;
    for (i = 0; i < MEMP_NUM_TCP_PCB; i++) {
        if (percpu_get(handle2pcb_array[i]) == NULL) {
            percpu_get(handle2pcb_array[i]) = pcb;
            break;
        }
    }

    assert(i < MEMP_NUM_TCP_PCB);
    hid_t hid = i;

    return hid;
}

static void recv_a_pbuf(struct tcpapi_pcb *api, struct pbuf *p) {

    // MEMPOOL_SANITY_LINK(api, p);

    // Walk through the full receive chain
    do {
        usys_tcp_recv(api->handle, api->cookie, p->payload, p->len);

        p = p->next;
    } while (p);
}

/*
 * Note: this fdir code works but better to do in setup vs. on accept
 *       thus, moved this code to ixgbe.c, called from ethdev.c
static int set_fdir_filter_on_accept(struct ip_tuple *id)
{
        int ret;
        struct rte_fdir_filter fdir_ftr;
        struct ix_rte_eth_dev *dev;
        struct eth_rx_queue *queue;
        int queue_id = 0;

        fdir_ftr.iptype = RTE_FDIR_IPTYPE_IPV4;
        fdir_ftr.l4type = RTE_FDIR_L4TYPE_TCP;

        fdir_ftr.ip_src.ipv4_addr = 172951061; //10.79.6.21 (mav-10)
        fdir_ftr.ip_dst.ipv4_addr = 172951158; //10.79.6.118
        fdir_ftr.port_src = 0;
        fdir_ftr.port_dst = 1234;
    queue_id = 0;
        queue = percpu_get(eth_rxqs[0]);
        dev = queue->dev;
        ret = dev->dev_ops->fdir_add_perfect_filter(dev, &fdir_ftr, 0, queue_id,
0); if (ret < 0){ log_err("ERROR setting flow director filter\n"); return -1;
        }

        fdir_ftr.ip_src.ipv4_addr = 172951060; //10.79.6.20 (mav-9)
        fdir_ftr.ip_dst.ipv4_addr = 172951158; //10.79.6.118
        fdir_ftr.port_src = 0;
        fdir_ftr.port_dst = 5678;
    queue_id = 1;
        queue = percpu_get(eth_rxqs[0]);
        dev = queue->dev;
        ret = dev->dev_ops->fdir_add_perfect_filter(dev, &fdir_ftr, 0, queue_id,
0); if (ret < 0){ log_err("ERROR setting flow director filter\n"); return -1;
        }
        return 0;
}
*/

long bsys_tcp_accept(hid_t handle, unsigned long cookie) {
    /*
     * FIXME: this function is sort of a placeholder since we have no
     * choice but to have already accepted the connection under LWIP's
     * synchronous API.
     */

    struct tcpapi_pcb *api = handle_to_tcpapi(handle);
    struct pbuf *tmp;

    KSTATS_VECTOR(bsys_tcp_accept);

    if (unlikely(!api)) {
        log_info("tcpapi: invalid handle\n");
        return -RET_BADH;
    }

    if (api->id) {
        mempool_free(&percpu_get(id_mempool), api->id);
        api->id = NULL;
    }

    api->cookie = cookie;
    api->accepted = true;

    tmp = api->recvd;
    while (tmp) {
        recv_a_pbuf(api, tmp);
        tmp = tmp->next;
    }

    return RET_OK;
}

long bsys_tcp_reject(hid_t handle) {
    /*
     * FIXME: LWIP's synchronous handling of accepts
     * makes supporting this call impossible.
     */

    KSTATS_VECTOR(bsys_tcp_reject);

    log_err("tcpapi: bsys_tcp_reject() is not implemented\n");

    return -RET_NOTSUP;
}

ssize_t bsys_tcp_send(hid_t handle, void *addr, size_t len) {
    KSTATS_VECTOR(bsys_tcp_send);

    log_debug("tcpapi: bsys_tcp_send() - addr %p, len %lx\n", addr, len);

    return -RET_NOTSUP;
}

ssize_t bsys_tcp_sendv(hid_t handle, struct sg_entry __user *ents,
                       unsigned int nrents) {
    struct tcpapi_pcb *api = handle_to_tcpapi(handle);
    int i;
    size_t len_xmited = 0;

    KSTATS_VECTOR(bsys_tcp_sendv);

    log_debug("tcpapi: bsys_tcp_sendv() - handle %lx, ents %p, nrents %ld\n",
              handle, ents, nrents);

    if (unlikely(!api)) {
        log_debug("tcpapi: invalid handle\n");
        return -RET_BADH;
    }

    if (unlikely(!api->alive)) return -RET_CLOSED;

    nrents = min(nrents, MAX_SG_ENTRIES);
    for (i = 0; i < nrents; i++) {
        err_t err;

        void *base = (void *)ents[i].base;
        size_t len = ents[i].len;
        bool buf_full = len > min(api->pcb->snd_buf, 0xFFFF);

        // Checks if memory object lies in userspace - not necessary anymore
        // if (unlikely(!uaccess_okay(base, len)))
        //	break;

        /*
         * FIXME: hacks to deal with LWIP's send buffering
         * design when handling large send requests. LWIP
         * buffers send data but in IX we don't want any
         * buffering in the kernel at all. Thus, the real
         * limit here should be the TCP cwd. Unfortunately
         * tcp_out.c needs to be completely rewritten to
         * support this.
         */
        if (buf_full) len = min(api->pcb->snd_buf, 0xFFFF);
        if (!len) break;

        /*
         * FIXME: Unfortunately LWIP's TX path is compeletely
         * broken in terms of zero-copy. It's also somewhat
         * broken in terms of large write requests. Here's a
         * hacky placeholder until we can rewrite this path.
         */
        err = tcp_write(api->pcb, base, len, 0);
        if (err != ERR_OK) break;

        len_xmited += len;
        if (buf_full) break;
    }

    if (len_xmited) tcp_output(api->pcb);

    return len_xmited;
}


// Application finished pulling len bytes from the receive buffer
long bsys_tcp_recv_done(hid_t handle, size_t len) {
    struct tcpapi_pcb *api = handle_to_tcpapi(handle);
    struct pbuf *recvd, *next;

    KSTATS_VECTOR(bsys_tcp_recv_done);

    log_debug("tcpapi: bsys_tcp_recv_done - handle %lx, len %lu\n", handle,
              len);
    assert(len), "tcpapi: bsys_tcp_recv_done - len is 0\n"; 

    if (unlikely(!api)) {
        log_debug("tcpapi: invalid handle\n");
        return -RET_BADH;
    }

    recvd = api->recvd;

    if (api->pcb) tcp_recved(api->pcb, len);

    while (recvd) {
        if (len < recvd->len) break;

        len -= recvd->len;
        next = recvd->next;
        // dechain the pbuf to avoid freeing the whole chain
        recvd->next = NULL;
        pbuf_free(recvd);
        recvd = next;
    }

    api->recvd = recvd;
    return RET_OK;
}

long bsys_tcp_close(hid_t handle) {
    struct tcpapi_pcb *api = handle_to_tcpapi(handle);
    struct pbuf *recvd, *next;

    KSTATS_VECTOR(bsys_tcp_close);

    log_debug("tcpapi: bsys_tcp_close - handle %lx\n", handle);

    if (unlikely(!api)) {
        log_debug("tcpapi: invalid handle\n");
        return -RET_BADH;
    }

    if (api->pcb) {
        tcp_close(api->pcb);
    }

    recvd = api->recvd;
    while (recvd) {
        next = recvd->next;
        recvd->next = NULL;
        pbuf_free(recvd);
        recvd = next;
    }

    if (api->id) {
        remove_fdir_filter(api->id);
        mempool_free(&percpu_get(id_mempool), api->id);
    }

    // Free spot in handle2pcb_array
    unsigned int i = 0;
    for (i = 0; i < MEMP_NUM_TCP_PCB; i++) {
        if (api == percpu_get(handle2pcb_array[i])) {
            percpu_get(handle2pcb_array[i]) = NULL;
            break;
        }
    }

    assert(i < MEMP_NUM_TCP_PCB);

    mempool_free(&percpu_get(pcb_api_mempool), api);
    return RET_OK;
}

static void mark_dead(struct tcpapi_pcb *api, unsigned long cookie) {
    if (!api) {
        usys_tcp_dead(0, cookie);
        return;
    }

    if (api->id) remove_fdir_filter(api->id);

    api->alive = false;
    usys_tcp_dead(api->handle, api->cookie);
}

static err_t on_recv(void *arg, struct tcp_pcb *pcb, struct pbuf *p,
                     err_t err) {
    struct tcpapi_pcb *api;

    log_debug("tcpapi: on_recv - arg %p, pcb %p, pbuf %p, err %d\n", arg, pcb,
              p, err);

    api = (struct tcpapi_pcb *)arg;

    /* FIXME: It's not really clear what to do with "err" */

    /* Was the connection closed? */
    if (!p) {
        mark_dead(api, api->cookie);
        return ERR_ABRT;
    }

    if (!api->recvd) {
        api->recvd = p;
        api->recvd_tail = p;
    } else {
        api->recvd_tail->next = p;
        api->recvd_tail = p;
    }
    p->next = NULL;

    /*
     * FIXME: This is a pretty annoying hack. LWIP accepts connections
     * synchronously while we have to wait for the app to accept the
     * connection. As a result, we have no choice but to assume the
     * connection will be accepted. Thus, we may start receiving data
     * packets before the app has allocated a recieve context and set
     * the appropriate cookie value. For now we wait for the app to
     * accept the connection before we allow receive events to be
     * sent. Clearly, the receive path needs to be rewritten.
     */
    if (!api->accepted) goto done;

    recv_a_pbuf(api, p);

done:
    return ERR_OK;
}

static void on_err(void *arg, err_t err) {
    struct tcpapi_pcb *api;
    unsigned long cookie;

    log_debug("tcpapi: on_err - arg %p err %d\n", arg, err);

    /* Because we use the LWIP_EVENT_API, LWIP can invoke on_err before we
     * invoke tcp_arg, thus arg will be NULL. This happens, e.g., if we
     * receive a RST after sending a SYN+ACK. */
    if (!arg) return;

    api = (struct tcpapi_pcb *)arg;
    cookie = api->cookie;

    if (err == ERR_ABRT || err == ERR_RST || err == ERR_CLSD) {
        mark_dead(api, cookie);
        api->pcb = NULL;
    }
}

static err_t on_sent(void *arg, struct tcp_pcb *pcb, u16_t len) {
    struct tcpapi_pcb *api;

    log_debug("tcpapi: on_sent - arg %p, pcb %p, len %hd\n", arg, pcb, len);

    api = (struct tcpapi_pcb *)arg;
    usys_tcp_sent(api->handle, api->cookie, len);

    return ERR_OK;
}

static err_t on_accept(void *arg, struct tcp_pcb *pcb, err_t err) {
    struct tcpapi_pcb *api;
    struct ip_tuple *id;
    hid_t handle;

    log_debug("tcpapi: on_accept - arg %p, pcb %p, err %d\n", arg, pcb, err);

    api = mempool_alloc(&percpu_get(pcb_api_mempool));
    if (unlikely(!api)) return ERR_MEM;
    id = mempool_alloc(&percpu_get(id_mempool));
    if (unlikely(!id)) {
        mempool_free(&percpu_get(pcb_api_mempool), api);
        return ERR_MEM;
    }

    api->pcb = pcb;
    api->alive = true;
    api->cookie = 0;
    api->recvd = NULL;
    api->recvd_tail = NULL;
    api->accepted = false;

    tcp_nagle_disable(pcb);
    tcp_arg(pcb, api);

#if LWIP_CALLBACK_API
    tcp_recv(pcb, on_recv);
    tcp_err(pcb, on_err);
    tcp_sent(pcb, on_sent);
#endif

    id->src_ip =
        ntoh32(pcb->remote_ip
                   .addr); /* FIXME: LWIP doesn't provide this information :( */
    id->dst_ip = CFG.host_addr.addr;
    id->src_port = pcb->remote_port;
    id->dst_port = pcb->local_port;
    api->id = id;
    handle = tcpapi_to_handle(api);
    api->handle = handle;

    usys_tcp_knock(handle, id);
    return ERR_OK;
}

static err_t on_connected(void *arg, struct tcp_pcb *pcb, err_t err) {
    struct tcpapi_pcb *api = (struct tcpapi_pcb *)arg;

    if (err != ERR_OK) {
        log_err("tcpapi: connection failed, ret %d\n", err);
        /* FIXME: free memory and mark handle dead */
        usys_tcp_connected(api->handle, api->cookie, RET_CONNREFUSED);
        return err;
    }

    usys_tcp_connected(api->handle, api->cookie, RET_OK);
    return ERR_OK;
}

/**
 * lwip_tcp_event -- "callback from the LWIP library
 */

err_t lwip_tcp_event(void *arg, struct tcp_pcb *pcb, enum lwip_event event,
                     struct pbuf *p, u16_t size, err_t err) {
    switch (event) {
        case LWIP_EVENT_ACCEPT:
            return on_accept(arg, pcb, err);
            break;
        case LWIP_EVENT_SENT:
            return on_sent(arg, pcb, size);
            break;
        case LWIP_EVENT_RECV:
            return on_recv(arg, pcb, p, err);
            break;
        case LWIP_EVENT_CONNECTED:
            return on_connected(arg, pcb, err);
            break;
        case LWIP_EVENT_ERR:
            on_err(arg, err);
            return 0;
            break;

        case LWIP_EVENT_POLL:
            return ERR_OK;
        default:
            assert(0);
    }
    return ERR_OK;
}

/* FIXME: we should maintain a bitmap to hold the available TCP ports */

/* FIXME:
   -- this is totally broken with flow-group migration.  The match should be
   based on a matching fgid for that device
   -- for multi-device bonds, need to also figure out (and reverse) the L3+L4
   bond that is in place.
   -- performance will be an issue as well with 1/128 probability of success
   (from 1/16).

   -- short version: need to fix this by using flow director for all outbound
   connections.  (EdB 2014-11-17)
*/

static uint32_t compute_toeplitz_hash(const uint8_t *key, uint32_t src_addr,
                                      uint32_t dst_addr, uint16_t src_port,
                                      uint16_t dst_port) {
    int i, j;
    uint8_t input[12];
    uint32_t result = 0;
    uint32_t key_part = htonl(((uint32_t *)key)[0]);

    memcpy(&input[0], &src_addr, 4);
    memcpy(&input[4], &dst_addr, 4);
    memcpy(&input[8], &src_port, 2);
    memcpy(&input[10], &dst_port, 2);

    for (i = 0; i < 12; i++) {
        for (j = 128; j; j >>= 1) {
            if (input[i] & j) result ^= key_part;
            key_part <<= 1;
            if (key[i + 4] & j) key_part |= 1;
        }
    }

    return result;
}

// FIXME: simplify fdir add and delete
static void remove_fdir_filter(struct ip_tuple *id) {
    // struct rte_fdir_filter fdir_ftr;
    // struct ix_rte_eth_dev *dev;
    // int ret = 0;

    // fdir_ftr.iptype = RTE_FDIR_IPTYPE_IPV4;
    // fdir_ftr.l4type = RTE_FDIR_L4TYPE_TCP;
    // fdir_ftr.ip_src.ipv4_addr = id->dst_ip;
    // fdir_ftr.ip_dst.ipv4_addr = id->src_ip;
    // fdir_ftr.port_src = id->dst_port;
    // fdir_ftr.port_dst = id->src_port;
    // dev = percpu_get(eth_rxqs[0])->dev;
    // ret = dev->dev_ops->fdir_remove_perfect_filter(dev, &fdir_ftr, 0);
    // if (ret < 0) {
    // 	log_err("cfg: failed to remove FDIR rule, ret %d.\n", ret);
    // 	return ret;
    // }
    int ret;
    struct rte_eth_fdir_filter filter;
    struct ix_rte_eth_dev *dev;
    struct eth_rx_queue *queue;

    filter.input.flow_type = RTE_ETH_FLOW_NONFRAG_IPV4_TCP;
    filter.input.flow.tcp4_flow.ip.src_ip = hton32(id->dst_ip);
    filter.input.flow.tcp4_flow.ip.dst_ip = hton32(id->src_ip);  // tos, ttl?
    filter.input.flow.tcp4_flow.src_port = hton16(id->dst_port);
    filter.input.flow.tcp4_flow.dst_port = hton16(id->src_port);
    filter.soft_id = 0;
    filter.action.rx_queue = percpu_get(cpu_id);
    filter.action.behavior = RTE_ETH_FDIR_ACCEPT;
    filter.action.report_status = RTE_ETH_FDIR_REPORT_ID;

    ret = rte_eth_dev_filter_ctrl(g_active_eth_port, RTE_ETH_FILTER_FDIR,
                                  RTE_ETH_FILTER_DELETE, &filter);
    if (ret < 0) {
        log_err("cfg: failed to remove FDIR rule, ret %d.\n", ret);
        return ret;
    }
}

static void get_port_with_fdir(struct ip_tuple *id) {
    int ret;
    struct rte_eth_fdir_filter filter;  // FIXEME: Try rte_eth_ntuple_filter
    struct ix_rte_eth_dev *dev;
    struct eth_rx_queue *queue;

    ret = rte_eth_dev_filter_supported(g_active_eth_port, RTE_ETH_FILTER_FDIR);
    if (ret < 0) {
        log_debug("WARNING: flow director not supported on this device!\n");
        return NULL;
    }

    filter.input.flow_type = RTE_ETH_FLOW_NONFRAG_IPV4_TCP;
    filter.input.flow.tcp4_flow.ip.src_ip = hton32(id->dst_ip);
    filter.input.flow.tcp4_flow.ip.dst_ip = hton32(id->src_ip);  // tos, ttl?
    filter.input.flow.tcp4_flow.src_port = hton16(id->dst_port);
    filter.input.flow.tcp4_flow.dst_port = hton16(id->src_port);
    filter.soft_id = 0;
    filter.action.rx_queue =
        percpu_get(cpu_id);  // FIXME: or should this always be 0?
    filter.action.behavior = RTE_ETH_FDIR_ACCEPT;
    filter.action.report_status = RTE_ETH_FDIR_REPORT_ID;

    ret = rte_eth_dev_filter_ctrl(g_active_eth_port, RTE_ETH_FILTER_FDIR,
                                  RTE_ETH_FILTER_ADD, &filter);
    if (ret < 0) {
        log_err("cfg: failed to add FDIR rule, ret %d.\n", ret);
        return ret;
    }
    log_info("FDIR: dst_ip %x, src_ip %x, dst_port %d <-- queue (core) %d\n",
             filter.input.flow.tcp4_flow.ip.dst_ip,
             filter.input.flow.tcp4_flow.ip.src_ip,
             filter.input.flow.tcp4_flow.dst_port, filter.action.rx_queue);
}

// this is called by client on dial
void get_local_port_and_set_queue(struct ip_tuple *id) {
    int ret;
    uint32_t hash;
    uint32_t fg_idx;
    // struct ix_rte_eth_dev *dev;
    struct rte_eth_rss_conf rss_conf;

    if (rte_eth_dev_count_avail() > 1)
        log_debug("WARNING: only 1 ethernet port is used\n");
    // panic("tcp_connect not implemented for bonded interfaces\n");

    if (!percpu_get(local_port))
        percpu_get(local_port) = RTE_PER_LCORE(cpu_id) * PORTS_PER_CPU;

    percpu_get(local_port)++;

    id->src_port = percpu_get(local_port);

    get_port_with_fdir(id);
}

long bsys_tcp_connect(struct ip_tuple __user *id, unsigned long cookie) {
    err_t err;
    struct ip_tuple tmp;
    ip_addr_t addr;
    struct tcp_pcb *pcb;
    struct tcpapi_pcb *api;

    KSTATS_VECTOR(bsys_tcp_connect);

    log_debug("tcpapi: bsys_tcp_connect() - id %p, cookie %lx\n", id, cookie);

    percpu_get(syscall_cookie) = cookie;

    tmp = *id;

    tmp.src_ip = CFG.host_addr.addr;

    pcb = tcp_new();
    if (unlikely(!pcb)) {
        log_debug("tcpapi: failed to allocate memory for pcb\n");
        return ERR_MEM;
    }
    tcp_nagle_disable(pcb);

    api = mempool_alloc(&percpu_get(pcb_api_mempool));
    if (unlikely(!api)) {
        log_debug("tcpapi: failed to allocate memory for pcb wrapper\n");
        tcp_abort(pcb);
        return ERR_MEM;
    }

    api->pcb = pcb;
    api->alive = true;
    api->cookie = cookie;
    api->recvd = NULL;
    api->recvd_tail = NULL;
    api->accepted = true;

    tcp_arg(pcb, api);

    api->handle = tcpapi_to_handle(api);

#if LWIP_CALLBACK_API
    tcp_recv(pcb, on_recv);
    tcp_err(pcb, on_err);
    tcp_sent(pcb, on_sent);
#endif

    addr.addr = hton32(tmp.src_ip);

    err = tcp_bind(pcb, &addr, tmp.src_port);
    if (unlikely(err != ERR_OK)) {
        if (err == ERR_USE) {
            log_debug("tcpapi: port %d in use\n", tmp.src_port);
        } else {
            log_debug("tcpapi: failed to bind to port %d, ret %d\n",
                      tmp.src_port, err);
        }
        tcp_abort(pcb);
        return err;
    }

    addr.addr = hton32(tmp.dst_ip);

    err = tcp_connect(pcb, &addr, tmp.dst_port, on_connected);
    if (unlikely(err != ERR_OK)) {
        tcp_abort(pcb);
        return err;
    }

    return api->handle;
}

#ifndef REFACTORING
/* derived from ip_output_hinted; a mess because of conflicts between LWIP and
 * IX */
extern int arp_lookup_mac(ip_addr_t *addr, struct eth_addr *mac);

int tcp_output_packet(struct tcp_pcb *pcb, struct pbuf *p) {
    int ret;
    struct rte_mbuf *pkt;
    struct eth_hdr *ethhdr;
    struct ip_hdr *iphdr;
    unsigned char *payload;
    struct pbuf *curp;
    ip_addr_t dst_addr;

    pkt = mbuf_alloc_local();
    if (unlikely(!(pkt))) return -ENOMEM;

    ethhdr = rte_pktmbuf_mtod(pkt, struct eth_hdr *);
    iphdr = mbuf_nextd(ethhdr, struct ip_hdr *);
    payload = mbuf_nextd(iphdr, unsigned char *);

    dst_addr.addr = ntoh32(pcb->remote_ip.addr);

    // setup IP hdr
    IPH_VHL_SET(iphdr, 4, sizeof(struct ip_hdr) / 4);
    // iphdr->header_len = sizeof(struct ip_hdr) / 4;
    // iphdr->version = 4;
    iphdr->_len = hton16(sizeof(struct ip_hdr) + p->tot_len);
    iphdr->_id = 0;
    iphdr->_offset = 0;
    iphdr->_proto = IP_PROTO_TCP;
    iphdr->_chksum = 0;
    iphdr->_tos = pcb->tos;
    iphdr->_ttl = pcb->ttl;
    iphdr->src.addr = pcb->local_ip.addr;
    iphdr->dest.addr = pcb->remote_ip.addr;

    // Offload IP and TCP tx checksums
    pkt->ol_flags |= PKT_TX_IP_CKSUM | PKT_TX_IPV4;
    pkt->ol_flags |= PKT_TX_TCP_CKSUM;
    // pkt->ol_flags |= PKT_TX_TCP_SEG; // if DEV_TX_OFFLOAD_TCP_TSO

    pkt->l2_len = sizeof(struct eth_hdr);
    pkt->l3_len = sizeof(struct ip_hdr);

    // p->cksum = rte_ipv4_phdr_cksum(iphdr, pkt->ol_flags); //FIXME: pseudo
    // header??

    for (curp = p; curp; curp = curp->next) {
        memcpy(payload, curp->payload, curp->len);
        payload += curp->len;
    }

    ret = ip_send_one(
        &dst_addr, pkt,
        sizeof(struct eth_hdr) + sizeof(struct ip_hdr) + p->tot_len);
    if (unlikely(ret)) {
        rte_pktmbuf_free(pkt);
        return -EIO;
    }

    return 0;
}
#endif

static err_t lwip_tcp_accept_cb(void *arg, struct tcp_pcb *newpcb, err_t err) {}

static err_t lwip_start_tcp_server_impl(const ip_addr_t *local_addr,
                                        u16_t local_port,
                                        struct tcp_pcb *server_pcb) {
    err_t err;
    struct tcp_pcb *pcb;

    LWIP_ASSERT_CORE_LOCKED();

    if (local_addr == NULL) {
        return ERR_ARG;
    }

    pcb = tcp_new_ip_type(IPADDR_TYPE_ANY);
    if (pcb == NULL) {
        return ERR_MEM;
    }
    err = tcp_bind(pcb, local_addr, local_port);
    if (err != ERR_OK) {
        return err;
    }
    server_pcb = tcp_listen_with_backlog(pcb, TCP_DEFAULT_LISTEN_BACKLOG);
    if (server_pcb == NULL) {
        if (pcb != NULL) {
            tcp_close(pcb);
        }
        return ERR_MEM;
    }
    pcb = NULL;

#if LWIP_CALLBACK_API
    tcp_accept(server_pcb, lwip_tcp_accept_cb);
    // tcp_recv(server_pcb, on_recv);
    // tcp_err(server_pcb, on_err);
    // tcp_sent(server_pcb, on_sent);
#endif /* LWIP_CALLBACK_API */
    return ERR_OK;
}

int tcp_api_init(void) {
    int ret;
    ret = mempool_create_datastore(&pcb_api_datastore, MEMP_NUM_TCP_PCB,
                                   sizeof(struct tcpapi_pcb), "api pcb");
    if (ret) {
        printf("tcp_api_init -- mempool_create_datastore failed\n");
        return ret;
    }

    if (pcb_api_datastore.elem_len != sizeof(struct tcpapi_pcb))
        panic("tcp_api_init -- wrong ELEM_LEN\n");

    ret = mempool_create_datastore(&id_datastore, MEMP_NUM_TCP_PCB,
                                   sizeof(struct ip_tuple), "ip");
    if (ret) {
        printf("tcp_api_init -- mempool_create_datastore failed\n");
        return ret;
    }

    lwip_init();

    return ret;
}

int tcp_api_init_cpu(void) {
    int ret;
    ret = mempool_create(&percpu_get(pcb_api_mempool), &pcb_api_datastore,
                         MEMPOOL_SANITY_PERCPU, RTE_PER_LCORE(cpu_id));
    if (ret) {
        printf("tcp_api_init_cpu -- mempool_create failed\n");
        return ret;
    }

    // Initializing per core array of tcpapi_pcb pointers
    unsigned int i;
    for (i = 0; i < MEMP_NUM_TCP_PCB; i++)
        percpu_get(handle2pcb_array[i]) = NULL;

    ret = mempool_create(&percpu_get(id_mempool), &id_datastore,
                         MEMPOOL_SANITY_PERCPU, RTE_PER_LCORE(cpu_id));
    if (ret) {
        printf("tcp_api_init_cpu -- mempool_create failed\n");
        return ret;
    }

    if (CFG.num_ports == 0) {
        // ret = tcp_listen_with_backlog(&percpu_get(listen_ports[0]),
        //                               TCP_DEFAULT_LISTEN_BACKLOG,
        //                               IP_ADDR_ANY, DEFAULT_PORT);
        ret = lwip_start_tcp_server_impl(IP_ADDR_ANY, DEFAULT_PORT,
                                         &percpu_get(listen_ports[0]));
        if (ret) {
            printf("tcp_api_init_cpu -- tcp_listen_with_backlog failed\n");
            return ret;
        }
    } else {
        int i;
        for (i = 0; i < CFG.num_ports; i++) {
            // ret = tcp_listen_with_backlog(&percpu_get(listen_ports[i]),
            //                               TCP_DEFAULT_LISTEN_BACKLOG,
            //                               IP_ADDR_ANY, CFG.ports[i]);
            ret = lwip_start_tcp_server_impl(IP_ADDR_ANY, CFG.ports[i],
                                             &percpu_get(listen_ports[i]));
            if (ret) {
                printf("tcp_api_init_cpu -- tcp_listen_with_backlog failed\n");
                return ret;
            }
        }
    }

    //	percpu_get(port8000).accept = on_accept;

    return 0;
}
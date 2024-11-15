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
 * net.c - the main file for the network subsystem
 */

#include "net.h"

#include <ix/cfg.h>
#include <ix/log.h>
#include <ix/stddef.h>

#ifdef REFACTORING

static void net_dump_cfg(void) {
    // FIXME: only supports on port
    struct netif *netif = &g_netif[g_active_eth_port];
    u32_t host_addr_hl = lwip_ntohl(CFG.host_addr.addr);
    u32_t broadcast_addr_hl = lwip_ntohl(CFG.broadcast_addr.addr);
    u32_t gateway_addr_hl = lwip_ntohl(CFG.gateway_addr.addr);
    u32_t mask_hl = lwip_ntohl(CFG.mask);
    ip4_addr_t host_addr = {.addr = host_addr_hl};
    // ip4_addr_t broadcast_addr = {.addr = broadcast_addr_hl};
    ip4_addr_t gateway_addr = {.addr = gateway_addr_hl};
    ip4_addr_t subnet_mask = {.addr = mask_hl};

    // netif_add(&g_netif[g_active_eth_port], &host_addr, &mask, &gateway_addr,
    // NULL, ethif_init, tcpip_input);
    netif_set_addr(netif, &host_addr, &subnet_mask, &gateway_addr);
    // not sure if this is the correct point to set the MAC address
    SMEMCPY(netif->hwaddr, &CFG.mac, ETH_HWADDR_LEN);

    log_info("net: using the following configuration:\n");

    log_info("\thost IP:\t%s\n", ipaddr_ntoa(&host_addr_hl));
    log_info("\tbroadcast IP:\t%s\n", ipaddr_ntoa(&broadcast_addr_hl));
    log_info("\tgateway IP:\t%s\n", ipaddr_ntoa(&gateway_addr_hl));
    log_info("\tsubnet mask:\t%s\n", ipaddr_ntoa(&mask_hl));
}

#else
static void net_dump_cfg(void) {
    char str[IP_ADDR_STR_LEN];
    struct ip_addr mask = {CFG.mask};

    log_info("net: using the following configuration:\n");

    ip_addr_to_str((struct ip_addr *)&CFG.host_addr, str);
    log_info("\thost IP:\t%s\n", str);
    ip_addr_to_str((struct ip_addr *)&CFG.broadcast_addr, str);
    log_info("\tbroadcast IP:\t%s\n", str);
    ip_addr_to_str((struct ip_addr *)&CFG.gateway_addr, str);
    log_info("\tgateway IP:\t%s\n", str);
    ip_addr_to_str(&mask, str);
    log_info("\tsubnet mask:\t%s\n", str);
}
#endif
/**
 * net_init - initializes the network stack
 *
 * Returns 0 if successful, otherwise fail.
 */
int net_init(void) {
    int ret;

    // ret = etharp_init();
    // if (ret) {
    //     log_err("net: failed to initialize arp\n");
    //     return ret;
    // }

    return 0;
}


/**
 * net_cfg - load the network configuration parameters
 *
 * Returns 0 if successful, otherwise fail.
 */
int net_init_cpu() {
    net_dump_cfg();

    return 0;
}

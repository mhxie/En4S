/**
 * @file
 * Dynamic pool memory manager
 *
 * lwIP has dedicated pools for many structures (netconn, protocol control
 blocks,
 * packet buffers, ...). All these pools are managed here.
 *
 * @defgroup mempool Memory pools
 * @ingroup infrastructure
 * Custom memory pools

 */

/*
 * Copyright (c) 2001-2004 Swedish Institute of Computer Science.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 3. The name of the author may not be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 * EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
 * OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This file is part of the lwIP TCP/IP stack.
 *
 * Author: Adam Dunkels <adam@sics.se>
 *
 */

#include "memp.h"

#include <string.h>

#include "lwip/opt.h"
#include "lwip/stats.h"
#include "lwip/sys.h"

/* Make sure we include everything we need for size calculation required by
 * memp_std.h */
#include "lwip/altcp.h"
#include "lwip/api.h"
#include "lwip/etharp.h"
#include "lwip/igmp.h"
#include "lwip/ip4_frag.h"
#include "lwip/netbuf.h"
#include "lwip/pbuf.h"
#include "lwip/priv/api_msg.h"
#include "lwip/priv/sockets_priv.h"
#include "priv/tcp_priv.h"
#include "lwip/priv/tcpip_priv.h"
#include "lwip/raw.h"
#include "lwip/tcp.h"
#include "lwip/timeouts.h"
#include "lwip/udp.h"
/* needed by default MEMP_NUM_SYS_TIMEOUT */
#include "lwip/dns.h"
#include "lwip/ip6_frag.h"
#include "lwip/mld6.h"
#include "lwip/netdb.h"
#include "lwip/priv/nd6_priv.h"
#include "netif/ppp/ppp_opts.h"

#define LWIP_MEMPOOL(name, num, size, desc) \
    LWIP_MEMPOOL_DECLARE(name, num, size, desc)
#include "lwip/priv/memp_std.h"

const struct mempool_desc *const memp_pools[MEMP_MAX] = {
#define LWIP_MEMPOOL(name, num, size, desc) &memp_##name,
#include "lwip/priv/memp_std.h"
};

#if MEMP_OVERFLOW_CHECK >= 1
#error "MEMP_OVERFLOW_CHECK is defined but not implemented."
#endif

#ifdef LWIP_HOOK_FILENAME
#error "LWIP_HOOK_FILENAME is defined but not implemented."
#endif

#ifdef MEM_USE_IX_POOLS
void mem_init(void) {
}
#endif

/**
 * Initialize custom memory pool with direct index access.
 *
 * @param desc pool to initialize
 *
 */
void mempool_init_pool(const struct mempool_desc *desc) {
    // Initialize the mempool datastore with the given parameters
    if (mempool_create_datastore(desc->ds, desc->num, desc->size, desc->desc) !=
        0) {
        printf("Error: Failed to initialize mempool datastore for pool %s\n",
               desc->desc);
        return;
    }

    // Create the mempool
    if (mempool_create(desc->pool, desc->ds, MEMPOOL_SANITY_PERCPU,
                       percpu_get(cpu_id)) != 0) {
        printf("Error: Failed to create mempool for pool %s\n", desc->desc);
        return;
    }

#if MEMP_STATS
    // Assuming stats structure exists and is initialized correctly
    desc->stats->avail = desc->num;
#endif

#if MEMP_STATS && (defined(LWIP_DEBUG) || LWIP_STATS_DISPLAY)
    desc->stats->name = desc->desc;
#endif
}

/**
 * Initializes lwIP built-in pools.
 * Related functions: memp_malloc, memp_free
 *
 * Carves out memp_memory into linked lists for each pool-type.
 */
void memp_init(void) {
    u16_t i;

    /* for every pool: */
    for (i = 0; i < LWIP_ARRAYSIZE(memp_pools); i++) {
        mempool_init_pool(memp_pools[i]);

#if LWIP_STATS && MEMP_STATS
        lwip_stats.memp[i] = memp_pools[i]->stats;
#endif
    }
}

static void *do_mempool_malloc_pool(struct mempool_desc *desc) {
    struct memp *memp;
    SYS_ARCH_DECL_PROTECT(old_level);

    SYS_ARCH_PROTECT(old_level);

    memp = mempool_alloc(desc->pool);

    if (memp != NULL) {
#if MEMP_STATS
        desc->stats->used++;
        if (desc->stats->used > desc->stats->max) {
            desc->stats->max = desc->stats->used;
        }
#endif
        SYS_ARCH_UNPROTECT(old_level);
        return memp;
    } else {
#if MEMP_STATS
        desc->stats->err++;
#endif
        SYS_ARCH_UNPROTECT(old_level);
        LWIP_DEBUGF(MEMP_DEBUG | LWIP_DBG_LEVEL_SERIOUS,
                    ("memp_malloc: out of memory in pool %s\n", desc->desc));
    }

    return NULL;
}

/**
 * Get an element from a custom pool.
 *
 * @param desc the pool to get an element from
 *
 * @return a pointer to the allocated memory or a NULL pointer on error
 */
void *mempool_malloc_pool(const struct mempool_desc *desc) {
    LWIP_ASSERT("invalid pool desc", desc != NULL);
    if (desc == NULL) {
        return NULL;
    }

    return do_mempool_malloc_pool(desc);
}

/**
 * Get an element from a specific pool.
 *
 * @param type the pool to get an element from
 *
 * @return a pointer to the allocated memory or a NULL pointer on error
 */
void *memp_malloc(memp_t type) {
    void *memp;
    LWIP_ERROR("memp_malloc: type < MEMP_MAX", (type < MEMP_MAX), return NULL;);

    memp = do_mempool_malloc_pool(memp_pools[type]);

    return memp;
}

static void do_mempool_free_pool(const struct mempool_desc *desc, void *mem) {
    struct memp *memp;
    SYS_ARCH_DECL_PROTECT(old_level);

    LWIP_ASSERT("memp_free: mem properly aligned",
                ((mem_ptr_t)mem % MEM_ALIGNMENT) == 0);

    SYS_ARCH_PROTECT(old_level);

#if MEMP_STATS
    desc->stats->used--;
#endif

    mempool_free(desc->pool, mem);

#if MEMP_SANITY_CHECK
    LWIP_ASSERT("memp sanity", memp_sanity(desc));
#endif /* MEMP_SANITY_CHECK */

    SYS_ARCH_UNPROTECT(old_level);
}

/**
 * Put a custom pool element back into its pool.
 *
 * @param desc the pool where to put mem
 * @param mem the memp element to free
 */
void mempool_free_pool(const struct mempool_desc *desc, void *mem) {
    LWIP_ASSERT("invalid pool desc", desc != NULL);
    if ((desc == NULL) || (mem == NULL)) {
        return;
    }

    do_mempool_free_pool(desc, mem);
}

/**
 * Put an element back into its pool.
 *
 * @param type the pool where to put mem
 * @param mem the memp element to free
 */
void memp_free(memp_t type, void *mem) {
    LWIP_ERROR("memp_free: type < MEMP_MAX", (type < MEMP_MAX), return;);

    if (mem == NULL) {
        return;
    }

    do_mempool_free_pool(memp_pools[type], mem);
}

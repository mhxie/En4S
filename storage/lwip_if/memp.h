/**
 * @file
 * Memory pool API
 */

/*
 * Copyright (c) 2001-2004 Swedish Institute of Computer Science.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
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
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
 * SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
 * OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
 * IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 * This file is part of the lwIP TCP/IP stack.
 *
 * Author: Adam Dunkels <adam@sics.se>
 *
 */

#ifndef LWIP_IF_MEMP_H
#define LWIP_IF_MEMP_H

#include "lwip/opt.h"
#include "ix/mempool.h"

#ifdef __cplusplus
extern "C" {
#endif

/* run once with empty definition to handle all custom includes in lwippools.h */
#define LWIP_MEMPOOL(name,num,size,desc)
#include "lwip/priv/memp_std.h"

/** Create the list of all memory pools managed by memp. MEMP_MAX represents a NULL pool at the end */
typedef enum {
#define LWIP_MEMPOOL(name,num,size,desc)  MEMP_##name,
#include "lwip/priv/memp_std.h"
  MEMP_MAX
} memp_t;

#include "lwip/priv/memp_priv.h"
// #if MEMP_STATS
// #define LWIP_MEMPOOL_DECLARE_STATS_INSTANCE(name) static struct stats_mem name;
// #define LWIP_MEMPOOL_DECLARE_STATS_REFERENCE(name) &name,
// #else
// #define LWIP_MEMPOOL_DECLARE_STATS_INSTANCE(name)
// #define LWIP_MEMPOOL_DECLARE_STATS_REFERENCE(name)
// #endif
#include "lwip/stats.h"

// #include "lwip/priv/memp_priv.h"

/** lwip/ix mempool helper struct */
struct mempool_desc {
  /** Textual description */
  const char *desc;
#if MEMP_STATS
  /** Statistics */
  struct stats_mem *stats;
#endif

  /** Size of elements in pool */
  const u16_t size;
  /** Number of elements in pool */
  const u16_t num;

  struct mempool_datastore *ds;
  struct mempool *pool;
};

extern const struct mempool_desc* const memp_pools[MEMP_MAX];

/**
 * @ingroup mempool
 * Declare prototype for private memory pool if it is used in multiple files
 */
#define LWIP_MEMPOOL_PROTOTYPE(name) extern const struct mempool_desc memp_ ## name

/**
 * @ingroup mempool
 * Declare a private memory pool
 * Private mempools example:
 * .h: only when pool is used in multiple .c files: LWIP_MEMPOOL_PROTOTYPE(my_private_pool);
 * .c:
 *   - in global variables section: LWIP_MEMPOOL_DECLARE(my_private_pool, 10, sizeof(foo), "Some description")
 *   - call ONCE before using pool (e.g. in some init() function): LWIP_MEMPOOL_INIT(my_private_pool);
 *   - allocate: void* my_new_mem = LWIP_MEMPOOL_ALLOC(my_private_pool);
 *   - free: LWIP_MEMPOOL_FREE(my_private_pool, my_new_mem);
 *
 * To relocate a pool, declare it as extern in cc.h. Example for GCC:
 *   extern u8_t \_\_attribute\_\_((section(".onchip_mem"))) memp_memory_my_private_pool_base[];
 */
// FIXME: use per-thread mempool to improve performance
#define LWIP_MEMPOOL_DECLARE(name,num,size,desc) \
  LWIP_MEMPOOL_DECLARE_STATS_INSTANCE(memp_stats_ ## name); \
  \
  static struct mempool_datastore datastore_ ## name; \
  \
  const struct mempool mempool_ ## name; \
  \
  const struct mempool_desc memp_ ## name = { \
    (desc), \
    LWIP_MEMPOOL_DECLARE_STATS_REFERENCE(memp_stats_ ## name) \
    (size), \
    (num), \
    &datastore_ ## name, \
    &mempool_ ## name \
  };

#ifdef MEM_USE_IX_POOLS
void mem_init(void);
#endif

#if MEM_USE_POOLS
/** This structure is used to save the pool one element came from.
 * This has to be defined here as it is required for pool size calculation. */
struct memp_malloc_helper
{
   memp_t poolnr;
#if MEMP_OVERFLOW_CHECK || (LWIP_STATS && MEM_STATS)
   u16_t size;
#endif /* MEMP_OVERFLOW_CHECK || (LWIP_STATS && MEM_STATS) */
};
#endif /* MEM_USE_POOLS */


/**
 * @ingroup mempool
 * Initialize a private memory pool
 */
#define LWIP_MEMPOOL_INIT(name)    mempool_init_pool(&memp_ ## name)
/**
 * @ingroup mempool
 * Allocate from a private memory pool
 */
#define LWIP_MEMPOOL_ALLOC(name)   mempool_malloc_pool(&memp_ ## name)
/**
 * @ingroup mempool
 * Free element from a private memory pool
 */
#define LWIP_MEMPOOL_FREE(name, x) mempool_free_pool(&memp_ ## name, (x))

void  memp_init(void);
void *memp_malloc(memp_t type);
void  memp_free(memp_t type, void *mem);

#ifdef __cplusplus
}
#endif

#endif /* LWIP_IF_MEMP_H */

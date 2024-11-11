/*
 * Copyright (c) 2019-2024, UC Santa Cruz
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

#include <bufopts.h>
#include <ix/atomic.h>
#include <ix/cfg.h>
#include <ix/errno.h>
#include <ix/kstats.h>
#include <ix/lock.h>
#include <ix/log.h>
#include <ix/mempool.h>
#include <ix/syscall.h>
#include <limits.h>
#include <math.h>
#include <nvme/nvmedev.h>
#include <nvme/sw_counter.h>
#include <nvme/sw_table.h>
#include <nvme/tenant_mgmt.h>
#include <rte_per_lcore.h>
#include <rte_timer.h>
#include <spdk/nvme.h>
#include <sys/socket.h>
#ifdef REFLEX_SCHED
#include <nvme/sw_queue.h>
#endif

// extern from ix/cfg.h
int NVME_READ_COST;
int NVME_WRITE_COST;

// extern from nvmedev.h
DEFINE_BITMAP(g_ioq_bitmap, MAX_NUM_IO_QUEUES);
DEFINE_BITMAP(g_nvme_fgs_bitmap, MAX_NVME_FLOW_GROUPS);
DEFINE_SPINLOCK(nvme_bitmap_lock);

static struct mempool_datastore request_datastore;
static struct mempool_datastore ctx_datastore;
#ifdef REFLEX_SCHED
struct mempool_datastore nvme_swq_datastore;
#endif
static struct spdk_nvme_ctrlr *nvme_ctrlr[CFG_MAX_NVMEDEV] = {NULL};
static long global_ns_id = 1;
static long global_ns_size = 1;
static long global_ns_sector_size = 1;
static long active_nvme_devices = 0;
static unsigned int cpu2ssd[CFG_MAX_NVMEDEV];

// struct pci_dev *g_nvme_dev[CFG_MAX_NVMEDEV];

#define MAX_OPEN_BATCH 32
#define SGL_PAGE_SIZE \
    4096  // should match PAGE_SIZE defined in dp/core/reflex_server.c
#define DEFAULT_IO_QUEUE_SIZE 256

RTE_DEFINE_PER_LCORE(int, open_ev[MAX_OPEN_BATCH]);
RTE_DEFINE_PER_LCORE(int, open_ev_ptr);
RTE_DEFINE_PER_LCORE(struct spdk_nvme_qpair *, qpair);
RTE_DEFINE_PER_LCORE(bool, mempool_initialized);

// int g_max_outstanding_requests = 512;
long g_outstanding_requests = 0;
long g_outstanding_lc_requests = 0;
int g_max_outstanding_wr = 512;
int g_wr_ratio_limit = 100;
int g_be_wr_ratio_limit = 100;
long g_outstanding_wr = 0;
long g_shared_deficit_limit = 0;
unsigned long d_received_requests = 0;
unsigned long d_last_received_requests = 0;
unsigned long long d_accumulated_tokens = 0;
unsigned long d_accumulated_cost = 0;
double d_accumulated_delta = 0.0;
unsigned long d_donation_count = 0;
unsigned long g_sched_interval;
struct nvme_flow_group g_nvme_fgs[MAX_NVME_FLOW_GROUPS];
int join_flow_start = 0;
int join_flow_end = 0;
int leave_flow_start = 0;
int leave_flow_end = 0;
struct nvme_flow_group_min pending_join_flows[MAX_NVME_FLOW_GROUPS];
long pending_leave_flows[MAX_NVME_FLOW_GROUPS];
struct nvme_sw_table *g_nvme_sw_table;
unsigned long global_token_rate =
    UINT_MAX;  // max token rate device can handle for current strictest latency
               // SLO
atomic_u64_t global_leftover_tokens = ATOMIC_INIT(0);  // shared token bucket
atomic_t global_be_token_rate_per_tenant =
    ATOMIC_INIT(0);  // token rate per best effort tenant
atomic_t global_token_rate_per_tenant =
    ATOMIC_INIT(0);  // token rate per best effort tenant
unsigned long global_LC_sum_token_rate =
    0;  // LC tenant token reservation summed across all LC tenants globally
unsigned long global_LC_sum_rate = 0;
double global_LC_sum_wr_rate = 0;
unsigned long global_num_best_effort_tenants =
    0;  // total num of best effort tenants
unsigned long global_num_lc_tenants =
    0;  // total num of latency critical tenants
unsigned long global_lc_boost_no_BE =
    0;  // fair share of leftover tokens that LC tenant can use when no BE
        // registered

static bool global_readonly_flag = true;

#define QSTATS_INTERVAL rte_get_timer_hz() / 2  // 1 second
#define SCHED_FREQUENCY 100000                  // assume 100K polling, 10us

static RTE_DEFINE_PER_LCORE(struct rte_timer, _qstats_timer);
static RTE_DEFINE_PER_LCORE(struct rte_timer, _sched_timer);
RTE_DEFINE_PER_LCORE(struct mempool,
                     request_mempool __attribute__((aligned(64))));
RTE_DEFINE_PER_LCORE(struct mempool, ctx_mempool __attribute__((aligned(64))));
RTE_DEFINE_PER_LCORE(unsigned long, received_nvme_completions);
RTE_DEFINE_PER_LCORE(struct en4s_tenant_mgmt, tenant_manager);
RTE_DEFINE_PER_LCORE(struct sw_counter, rw_counter);
RTE_DEFINE_PER_LCORE(unsigned long, local_extra_demand);
RTE_DEFINE_PER_LCORE(unsigned long, local_leftover_tokens);

#ifdef REFLEX_SCHED
RTE_DEFINE_PER_LCORE(struct reflex_tenant_mgmt, reflex_tenant_manager);
RTE_DEFINE_PER_LCORE(unsigned long, last_sched_us);
RTE_DEFINE_PER_LCORE(unsigned long, last_sched_us_be);
RTE_DEFINE_PER_LCORE(struct nvme_sw_queue, g_default_swq);
#endif
RTE_DEFINE_PER_LCORE(unsigned long, last_sched_cycles_lc);
RTE_DEFINE_PER_LCORE(unsigned long, last_sched_cycles_be);
RTE_DEFINE_PER_LCORE(unsigned long, g_sched_time);
RTE_DEFINE_PER_LCORE(unsigned long, last_sys_bpoll_time);
RTE_DEFINE_PER_LCORE(double, flow_credit);

// static unsigned long g_sched_interval;\  // 1ms

static void set_token_deficit_limit(void);

struct nvme_ctx *alloc_local_nvme_ctx(void) {
    return mempool_alloc(&percpu_get(ctx_mempool));
}

extern void free_local_nvme_ctx(struct nvme_ctx *req) {
    mempool_free(&percpu_get(ctx_mempool), req);
}

static unsigned long find_token_limit_from_devmodel(unsigned int lat_SLO) {
    int i = 0;
    unsigned long y0, y1, x0, x1;
    double y;

    for (i = 0; i < g_dev_model_size; i++) {
        if (lat_SLO < g_dev_models[i].p95_tail_latency) {
            break;
        }
    }
    if (i > 0) {
        if (global_readonly_flag) {
            if (i == g_dev_model_size) {
                return g_dev_models[i - 1].token_rdonly_rate_limit;
            }
            // linear interpolation of token limits provided in devmodel config
            // file
            y0 = g_dev_models[i - 1].token_rdonly_rate_limit;
            y1 = g_dev_models[i].token_rdonly_rate_limit;
            x0 = g_dev_models[i - 1].p95_tail_latency;
            x1 = g_dev_models[i].p95_tail_latency;
            assert(x1 - x0 != 0);
            y = y0 + ((y1 - y0) * (lat_SLO - x0) / (double)(x1 - x0));
            return (unsigned long)y;

        } else {
            if (i == g_dev_model_size) {
                return g_dev_models[i - 1].token_rate_limit;
            }
            // linear interpolation of token limits provided in devmodel config
            // file
            y0 = g_dev_models[i - 1].token_rate_limit;
            y1 = g_dev_models[i].token_rate_limit;
            x0 = g_dev_models[i - 1].p95_tail_latency;
            x1 = g_dev_models[i].p95_tail_latency;
            y = y0 + ((y1 - y0) * (lat_SLO - x0) / (double)(x1 - x0));
            assert(x1 - x0 != 0);
            return (unsigned long)y;
        }
    }
    printf("WARNING: provide dev model info for latency SLO %d\n", lat_SLO);
    if (global_readonly_flag) {
        return g_dev_models[0].token_rdonly_rate_limit;
    }
    return g_dev_models[0].token_rate_limit;
}

unsigned long lookup_device_token_rate(unsigned int lat_SLO) {
    switch (g_nvme_dev_model) {
        case DEFAULT_FLASH:
            return UINT_MAX;
        case FAKE_FLASH:
            return UINT_MAX;
        case FLASH_DEV_MODEL:
            return find_token_limit_from_devmodel(lat_SLO);
        default:
            printf("WARNING: undefined flash device model\n");
            return UINT_MAX;
    }

    log_err("Set device lookup for lat SLO %ld!\n", lat_SLO);
    return 500000;
}

#ifdef REFLEX_SCHED
void print_reflex_queue_status() {
    struct reflex_tenant_mgmt *thread_tenant_manager_old =
        &percpu_get(reflex_tenant_manager);
    struct nvme_sw_queue *swq;
    printf("-------- Scheduling STATS --------\n");
    list_for_each(&thread_tenant_manager_old->tenant_swq_head, swq, link) {
        if (g_nvme_fgs[swq->fg_handle].latency_critical_flag) {
            printf(
                "LC Tenant %d has %d requests in the queue, remaining tokens = "
                "%ld\n",
                swq->fg_handle, swq->count, swq->token_credit);
        } else {
            printf(
                "BE Tenant %d has %d requests in the queue, local leftover = "
                "%lu\n",
                swq->fg_handle, swq->count, percpu_get(local_leftover_tokens));
            printf("Extra demand = %lu, saved tokens = %lu\n",
                   percpu_get(local_extra_demand), swq->saved_tokens);
            printf("Global leftover tokens = %lu\n",
                   atomic_u64_read(&global_leftover_tokens));
        }
        printf("History credit for this tenant is %ld\n", swq->history_credit);
    }
}
#endif

void print_queue_status() {
    struct en4s_tenant_mgmt *thread_tenant_manager =
        &percpu_get(tenant_manager);
    long fg_handle;
    struct nvme_flow_group *nvme_fg;

    printf(
        "There are still %ld requests pending in the table, %d requests in the "
        "SSD queue. See the snapshot below:\n",
        g_nvme_sw_table->total_request_count, g_outstanding_requests);
    printf("Total received requests = %lu, rate = %lu/second\n",
           d_received_requests, d_received_requests - d_last_received_requests);
    printf(
        "Accumulated tokens = %llu, cost = %lu, elapsed = %.2lfs, #donations = "
        "%lu\n",
        d_accumulated_tokens, d_accumulated_cost,
        d_accumulated_delta / (double)1E6, d_donation_count);
    d_last_received_requests = d_received_requests;

    iterate_all_tenants(fg_handle) {
        nvme_fg = bitmap_test(g_nvme_fgs_bitmap, fg_handle)
                      ? &g_nvme_fgs[fg_handle]
                      : NULL;
        if (nvme_fg != NULL) {
            printf("%ld-queue has %ld requests, ", fg_handle,
                   nvme_sw_table_count(g_nvme_sw_table, fg_handle));
            printf("demands = %lu, saved_tokens = %lu, token credit = %lld.\n",
                   g_nvme_sw_table->total_token_demand[fg_handle],
                   g_nvme_sw_table->saved_tokens[fg_handle],
                   g_nvme_sw_table->token_credit[fg_handle]);
        }
    }
    printf("Queue: ");
    iterate_active_tenants_by_type(thread_tenant_manager, lc) {
        fg_handle =
            thread_tenant_manager->active_lc_tenants[i % MAX_NVME_FLOW_GROUPS];
        printf("%ld, ", fg_handle);
    }
    printf(" are still active.\n");
}

/**
 * init_nvme_request_cpu - allocates the core-local nvme request region
 *
 * Returns 0 if successful, otherwise failure.
 */
int init_nvme_request_cpu(void) {
    struct en4s_tenant_mgmt *thread_tenant_manager =
        &percpu_get(tenant_manager);
    struct sw_counter *thread_rw_counter = &percpu_get(rw_counter);
#ifdef REFLEX_SCHED
    struct reflex_tenant_mgmt *thread_tenant_manager_old =
        &percpu_get(reflex_tenant_manager);
#endif
    int ret;

    if (percpu_get(mempool_initialized)) {
        return 0;
    }

    if (CFG.num_nvmedev == 0 || CFG.ns_sizes[0] != 0) {
        printf("No NVMe devices found, skipping initialization\n");
        return 0;
    }

    struct mempool *m2 = &percpu_get(ctx_mempool);
    ret = mempool_create(m2, &ctx_datastore, MEMPOOL_SANITY_PERCPU,
                         percpu_get(cpu_id));
    if (ret) {
        // FIXME: implement mempool destroy
        // mempool_destroy(m);
        return ret;
    }

#ifdef REFLEX_SCHED
    ret = init_local_nvme_swq_mempool();
    if (ret) {
        return ret;
    }
    thread_tenant_manager_old = &percpu_get(reflex_tenant_manager);
    list_head_init(&thread_tenant_manager_old->tenant_swq_head);
    thread_tenant_manager_old->num_tenants = 0;
    thread_tenant_manager_old->num_best_effort_tenants = 0;
    percpu_get(g_sched_time) = rdtsc();
    g_sched_interval = rte_get_timer_hz() / SCHED_FREQUENCY;

    nvme_sw_queue_init(&percpu_get(g_default_swq), 0);

#endif

    init_en4s_tenant_mgmt(thread_tenant_manager);
    init_counter(thread_rw_counter, DEFAULT_COUNTER_SIZE);
    percpu_get(last_sched_cycles_lc) = rdtsc();

    percpu_get(local_leftover_tokens) = 0;
    percpu_get(local_extra_demand) = 0;
    percpu_get(mempool_initialized) = true;

    // if (g_nvme_sched_mode) {
    //     g_sched_interval = rte_get_timer_hz() / SCHED_FREQUENCY;
    //     rte_timer_init(&percpu_get(_sched_timer));
    //     rte_timer_reset(&percpu_get(_sched_timer), g_sched_interval,
    //     PERIODICAL,
    //                     rte_lcore_id(), nvme_sched, NULL);
    // }
    // hard-coded with strictest SLO, otherwise no initial value
    global_token_rate = lookup_device_token_rate(3000);

    return ret;
}

/**
 * init_nvme_request- allocate global nvme request mempool
 */
int init_nvme_request(void) {
    int ret;
    struct mempool_datastore *m = &request_datastore;
    struct mempool_datastore *m2 = &ctx_datastore;

#ifdef REFLEX_SCHED
    struct mempool_datastore *m3 = &nvme_swq_datastore;

    // memory for software queues for nvme scheduling
    ret = mempool_create_datastore(
        m3,
        (MEMPOOL_DEFAULT_CHUNKSIZE * MAX_NVME_FLOW_GROUPS) /
            MEMPOOL_DEFAULT_CHUNKSIZE * 2,
        sizeof(struct nvme_sw_queue), "nvme_swq");
    if (ret) {
        // mempool_pagemem_destroy(m);
        return ret;
    }
#endif

    if (CFG.num_nvmedev == 0 || CFG.ns_sizes[0] != 0) {
        return 0;
    }

    ret = mempool_create_datastore(m2, NVME_MEMP_SZ, sizeof(struct nvme_ctx),
                                   "nvme_ctx");
    if (ret) {
        // mempool_pagemem_destroy(m);
        return ret;
    }

    // need to alloc req mempool for admin queue
    init_nvme_request_cpu();
    nvme_sw_table_init(&g_nvme_sw_table);
    if (g_nvme_sw_table == NULL) {
        panic("ERROR: failed to allocate nvme_sw_table\n");
        return -RET_NOMEM;
    }
#ifdef ENABLE_KSTATS
    rte_timer_init(&percpu_get(_qstats_timer));
    rte_timer_reset(&percpu_get(_qstats_timer), QSTATS_INTERVAL, PERIODICAL,
                    rte_lcore_id(), print_queue_status, NULL);
#endif
#ifdef REFLEX_SCHED
#ifdef SCHED_DEBUG
    rte_timer_init(&percpu_get(_qstats_timer));
    rte_timer_reset(&percpu_get(_qstats_timer), QSTATS_INTERVAL, PERIODICAL,
                    rte_lcore_id(), print_reflex_queue_status, NULL);
#endif
#endif
    // set_token_deficit_limit();

    return 0;
}

/**
 * nvme_request_exit_cpu - frees the core-local nvme request region
 */
void nvme_request_exit_cpu(void) {
    // mempool_pagemem_destroy(&request_datastore);
    // mempool_pagemem_destroy(&ctx_datastore);
    // mempool_pagemem_destroy(&nvme_swq_datastore);
}

static bool probe_cb(void *cb_ctx, struct spdk_pci_device *dev,
                     struct spdk_nvme_ctrlr_opts *opts) {
    printf("probe return\n");
    if (dev == NULL) {
        log_err("nvmedev: failed to start driver\n");
        return -ENODEV;
    }

    printf("attaching to nvme device\n");
    return true;
}

static void attach_cb(void *cb_ctx, struct spdk_pci_device *dev,
                      struct spdk_nvme_ctrlr *ctrlr,
                      const struct spdk_nvme_ctrlr_opts *opts) {
    unsigned int num_ns, nsid;
    const struct spdk_nvme_ctrlr_data *cdata;
    struct spdk_nvme_ns *ns = spdk_nvme_ctrlr_get_ns(ctrlr, 1);

    bitmap_init(g_ioq_bitmap, MAX_NUM_IO_QUEUES, 0);
    if (active_nvme_devices < CFG_MAX_NVMEDEV) {
        nvme_ctrlr[active_nvme_devices++] = ctrlr;
    } else {
        panic("ERROR: only support %d nvme devices\n", CFG_MAX_NVMEDEV);
        return -RET_INVAL;
    }
    cdata = spdk_nvme_ctrlr_get_data(ctrlr);

    if (!spdk_nvme_ns_is_active(ns)) {
        printf("Controller %-20.20s (%-20.20s): Skipping inactive NS %u\n",
               cdata->mn, cdata->sn, spdk_nvme_ns_get_id(ns));
        return;
    }

    printf("Attached to device %-20.20s (%-20.20s) controller: %p\n", cdata->mn,
           cdata->sn, ctrlr);

    num_ns = spdk_nvme_ctrlr_get_num_ns(ctrlr);
    printf("Found %i namespaces\n", num_ns);
    for (nsid = 1; nsid <= num_ns; nsid++) {
        struct spdk_nvme_ns *ns = spdk_nvme_ctrlr_get_ns(ctrlr, nsid);
        printf("NS: %i, size: %lx\n", nsid, spdk_nvme_ns_get_size(ns));
    }
}

/**
 * nvmedev_init - initializes nvme devices
 *
 * Returns 0 if successful, otherwise fail.
 */
int init_nvmedev(void) {
    // if (CFG.num_nvmedev > 1)
    // 	printf("IX supports only one NVME device, ignoring all further
    // devices\n");
    if (CFG.num_nvmedev == 0 || CFG.ns_sizes[0] != 0) {
        return 0;
    } else if (CFG.num_nvmedev > g_cores_active) {
        // panic("ERROR: cores are fewer than SSDs\n");
        printf("WARNING: %d nvme devices are not available.\n",
               CFG.num_nvmedev - g_cores_active);
    }

    int i;
    int cpu_per_ssd =
        ceil((double)g_cores_active /
             CFG.num_nvmedev);  // #core should be a multiple of #nvme devices
    for (i = 0; i < CFG.num_nvmedev; i++) {
        cpu2ssd[i] = i / cpu_per_ssd;
    }
    printf("Each SSD will be processed by %d cores.", cpu_per_ssd);

    if (spdk_nvme_probe(NULL, NULL, probe_cb, attach_cb, NULL) != 0) {
        printf("spdk_nvme_probe() failed\n");
        return 1;
    }
    return 0;
}

int init_nvmeqp_cpu(void) {
    if (CFG.num_nvmedev == 0 || CFG.ns_sizes[0] != 0) return 0;
    assert(nvme_ctrlr);
    struct spdk_nvme_ctrlr *ctrlr = nvme_ctrlr[cpu2ssd[percpu_get(cpu_id)]];
    struct spdk_nvme_io_qpair_opts opts;

    spdk_nvme_ctrlr_get_default_io_qpair_opts(ctrlr, &opts, sizeof(opts));
    printf("Deafult io qpair opts: %d, %d, %d\n", opts.qprio,
           opts.io_queue_size, opts.io_queue_requests);
    // opts.qprio = 0;
    opts.io_queue_size = opts.io_queue_size;
    opts.io_queue_requests = opts.io_queue_requests;
    // g_max_outstanding_requests = opts.io_queue_requests;

    percpu_get(qpair) =
        spdk_nvme_ctrlr_alloc_io_qpair(ctrlr, &opts, sizeof(opts));

    assert(percpu_get(qpair));

    return 0;
}

void nvmedev_exit(void) {
    struct spdk_nvme_ctrlr *nvme = nvme_ctrlr[cpu2ssd[percpu_get(cpu_id)]];
    if (!nvme) return;
}

int allocate_nvme_ioq(void) {
    int q;

    spin_lock(&nvme_bitmap_lock);
    for (q = 1; q < MAX_NUM_IO_QUEUES; q++) {
        if (bitmap_test(g_ioq_bitmap, q)) continue;
        bitmap_set(g_ioq_bitmap, q);
        break;
    }
    spin_unlock(&nvme_bitmap_lock);

    if (q == MAX_NUM_IO_QUEUES) {
        return -ENOMEM;
    }

    return q;
}

struct nvme_string {
    uint16_t value;
    const char *str;
};
static const struct nvme_string generic_status[] = {
    {SPDK_NVME_SC_SUCCESS, "SUCCESS"},
    {SPDK_NVME_SC_INVALID_OPCODE, "INVALID OPCODE"},
    {SPDK_NVME_SC_INVALID_FIELD, "INVALID_FIELD"},
    {SPDK_NVME_SC_COMMAND_ID_CONFLICT, "COMMAND ID CONFLICT"},
    {SPDK_NVME_SC_DATA_TRANSFER_ERROR, "DATA TRANSFER ERROR"},
    {SPDK_NVME_SC_ABORTED_POWER_LOSS, "ABORTED - POWER LOSS"},
    {SPDK_NVME_SC_INTERNAL_DEVICE_ERROR, "INTERNAL DEVICE ERROR"},
    {SPDK_NVME_SC_ABORTED_BY_REQUEST, "ABORTED - BY REQUEST"},
    {SPDK_NVME_SC_ABORTED_SQ_DELETION, "ABORTED - SQ DELETION"},
    {SPDK_NVME_SC_ABORTED_FAILED_FUSED, "ABORTED - FAILED FUSED"},
    {SPDK_NVME_SC_ABORTED_MISSING_FUSED, "ABORTED - MISSING FUSED"},
    {SPDK_NVME_SC_INVALID_NAMESPACE_OR_FORMAT, "INVALID NAMESPACE OR FORMAT"},
    {SPDK_NVME_SC_COMMAND_SEQUENCE_ERROR, "COMMAND SEQUENCE ERROR"},
    {SPDK_NVME_SC_LBA_OUT_OF_RANGE, "LBA OUT OF RANGE"},
    {SPDK_NVME_SC_CAPACITY_EXCEEDED, "CAPACITY EXCEEDED"},
    {SPDK_NVME_SC_NAMESPACE_NOT_READY, "NAMESPACE NOT READY"},
    {0xFFFF, "GENERIC"}};

static const struct nvme_string command_specific_status[] = {
    {SPDK_NVME_SC_COMPLETION_QUEUE_INVALID, "INVALID COMPLETION QUEUE"},
    {SPDK_NVME_SC_INVALID_QUEUE_IDENTIFIER, "INVALID QUEUE IDENTIFIER"},
    {SPDK_NVME_SC_INVALID_QUEUE_SIZE, "INVALID QUEUE SIZE"},
    {SPDK_NVME_SC_ABORT_COMMAND_LIMIT_EXCEEDED, "ABORT CMD LIMIT EXCEEDED"},
    {SPDK_NVME_SC_ASYNC_EVENT_REQUEST_LIMIT_EXCEEDED, "ASYNC LIMIT EXCEEDED"},
    {SPDK_NVME_SC_INVALID_FIRMWARE_SLOT, "INVALID FIRMWARE SLOT"},
    {SPDK_NVME_SC_INVALID_FIRMWARE_IMAGE, "INVALID FIRMWARE IMAGE"},
    {SPDK_NVME_SC_INVALID_INTERRUPT_VECTOR, "INVALID INTERRUPT VECTOR"},
    {SPDK_NVME_SC_INVALID_LOG_PAGE, "INVALID LOG PAGE"},
    {SPDK_NVME_SC_INVALID_FORMAT, "INVALID FORMAT"},
    {SPDK_NVME_SC_CONFLICTING_ATTRIBUTES, "CONFLICTING ATTRIBUTES"},
    {SPDK_NVME_SC_INVALID_PROTECTION_INFO, "INVALID PROTECTION INFO"},
    {SPDK_NVME_SC_ATTEMPTED_WRITE_TO_RO_RANGE, "WRITE TO RO RANGE"},
    {0xFFFF, "COMMAND SPECIFIC"}};

static const struct nvme_string media_error_status[] = {
    {SPDK_NVME_SC_WRITE_FAULTS, "WRITE FAULTS"},
    {SPDK_NVME_SC_UNRECOVERED_READ_ERROR, "UNRECOVERED READ ERROR"},
    {SPDK_NVME_SC_GUARD_CHECK_ERROR, "GUARD CHECK ERROR"},
    {SPDK_NVME_SC_APPLICATION_TAG_CHECK_ERROR, "APPLICATION TAG CHECK ERROR"},
    {SPDK_NVME_SC_REFERENCE_TAG_CHECK_ERROR, "REFERENCE TAG CHECK ERROR"},
    {SPDK_NVME_SC_COMPARE_FAILURE, "COMPARE FAILURE"},
    {SPDK_NVME_SC_ACCESS_DENIED, "ACCESS DENIED"},
    {0xFFFF, "MEDIA ERROR"}};

static const char *nvme_get_string(const struct nvme_string *strings,
                                   uint16_t value) {
    const struct nvme_string *entry;

    entry = strings;

    while (entry->value != 0xFFFF) {
        if (entry->value == value) {
            return entry->str;
        }
        entry++;
    }
    return entry->str;
}

static const char *get_status_string(uint16_t sct, uint16_t sc) {
    const struct nvme_string *entry;

    switch (sct) {
        case SPDK_NVME_SCT_GENERIC:
            entry = generic_status;
            break;
        case SPDK_NVME_SCT_COMMAND_SPECIFIC:
            entry = command_specific_status;
            break;
        case SPDK_NVME_SCT_MEDIA_ERROR:
            entry = media_error_status;
            break;
        case SPDK_NVME_SCT_VENDOR_SPECIFIC:
            return "VENDOR SPECIFIC";
        default:
            return "RESERVED";
    }

    return nvme_get_string(entry, sc);
}

void nvme_write_cb(void *ctx, const struct spdk_nvme_cpl *cpl) {
    struct nvme_ctx *n_ctx = (struct nvme_ctx *)ctx;
    struct sw_counter *thread_rw_counter = &percpu_get(rw_counter);

    g_nvme_fgs[n_ctx->fg_handle].completions++;
    g_outstanding_wr--;
    if (n_ctx->lc_req) g_outstanding_lc_requests--;
    counter_insert(thread_rw_counter, 1);

    if (spdk_nvme_cpl_is_error(cpl)) {
        printf("SPDK Write Failed!\n");
        printf(
            "%s (%02x/%02x) sqid:%d cid:%d cdw0:%x sqhd:%04x p:%x m:%x "
            "dnr:%x\n",
            get_status_string(cpl->status.sct, cpl->status.sc), cpl->status.sct,
            cpl->status.sc, cpl->sqid, cpl->cid, cpl->cdw0, cpl->sqhd,
            cpl->status.p, cpl->status.m, cpl->status.dnr);
    }

    usys_nvme_written(n_ctx->cookie, RET_OK);

    free_local_nvme_ctx(n_ctx);
}

void nvme_read_cb(void *ctx, const struct spdk_nvme_cpl *cpl) {
    struct nvme_ctx *n_ctx = (struct nvme_ctx *)ctx;
    struct sw_counter *thread_rw_counter = &percpu_get(rw_counter);

    g_nvme_fgs[n_ctx->fg_handle].completions++;
    counter_insert(thread_rw_counter, 0);
    if (n_ctx->lc_req) g_outstanding_lc_requests--;

    if (spdk_nvme_cpl_is_error(cpl)) {
        printf("SPDK Read Failed!\n");
        printf(
            "%s (%02x/%02x) sqid:%d cid:%d cdw0:%x sqhd:%04x p:%x m:%x "
            "dnr:%x\n",
            get_status_string(cpl->status.sct, cpl->status.sc), cpl->status.sct,
            cpl->status.sc, cpl->sqid, cpl->cid, cpl->cdw0, cpl->sqhd,
            cpl->status.p, cpl->status.m, cpl->status.dnr);
    }

    usys_nvme_response(n_ctx->cookie, n_ctx->user_buf.buf, RET_OK);

    free_local_nvme_ctx(n_ctx);
}

long bsys_nvme_open(long dev_id, long ns_id) {
    struct spdk_nvme_ns *ns;
    int ioq;

    KSTATS_VECTOR(bsys_nvme_open);

    // FIXME: we may want 1 bitmap per device
    ioq = allocate_nvme_ioq();
    if (ioq < 0) {
        return -RET_NOBUFS;
    }
    bitmap_init(g_nvme_fgs_bitmap, MAX_NVME_FLOW_GROUPS, 0);

    percpu_get(open_ev[percpu_get(open_ev_ptr)++]) = ioq;
    ns = spdk_nvme_ctrlr_get_ns(nvme_ctrlr[cpu2ssd[percpu_get(cpu_id)]], ns_id);
    global_ns_size = spdk_nvme_ns_get_size(ns);
    global_ns_sector_size = spdk_nvme_ns_get_sector_size(ns);
    printf("NVMe device namespace size: %lu bytes, sector size: %lu\n",
           spdk_nvme_ns_get_size(ns), spdk_nvme_ns_get_sector_size(ns));
    return RET_OK;
}

long bsys_nvme_close(long dev_id, long ns_id, hqu_t handle) {
    KSTATS_VECTOR(bsys_nvme_close);
    printf("BSYS NVME CLOSE\n");
    bitmap_clear(g_ioq_bitmap, handle);
    usys_nvme_closed(handle, 0);
    return RET_OK;
}

// adjust token deficit limit to allow LC tenants to burst, but not too much
static void set_token_deficit_limit(void) {
    printf(
        "DEVICE PARAMS: read cost %d, write cost %d, burst limits = %d "
        "writes\n",
        NVME_READ_COST, NVME_WRITE_COST, WRITE_BURST_COUNT);
    TOKEN_DEFICIT_LIMIT = WRITE_BURST_COUNT * NVME_READ_COST;
}

static void readjust_lc_tenant_token_limits(void) {
    int i, j = 0;
    for (i = 0; i < MAX_NVME_FLOW_GROUPS; i++) {
        if (bitmap_test(g_nvme_fgs_bitmap, i)) {
            if (g_nvme_fgs[i].latency_critical_flag) {
                g_nvme_fgs[i].scaled_IOPuS_limit =
                    (g_nvme_fgs[i].scaled_IOPS_limit + global_lc_boost_no_BE) /
                    (double)1E6;
                j++;
                if (j == global_num_lc_tenants) {
                    return;
                }
            }
        }
    }
}

int recalculate_weights_add(long new_flow_group_idx) {
    unsigned long new_global_token_rate = 0;
    unsigned long new_global_LC_sum_token_rate = 0;
    unsigned long lc_token_rate_boost_when_no_BE = 0;
    unsigned int be_token_rate_per_tenant, token_rate_per_tenant;

    spin_lock(&nvme_bitmap_lock);

    if (g_nvme_fgs[new_flow_group_idx].latency_critical_flag) {
        new_global_LC_sum_token_rate =
            global_LC_sum_token_rate +
            g_nvme_fgs[new_flow_group_idx].scaled_IOPS_limit;
        if (g_nvme_fgs[new_flow_group_idx].rw_ratio_SLO < 100) {
            global_readonly_flag = false;
        }
        global_LC_sum_wr_rate =
            global_LC_sum_wr_rate +
            g_nvme_fgs[new_flow_group_idx].IOPS_SLO *
                (100 - g_nvme_fgs[new_flow_group_idx].rw_ratio_SLO) / 100;
        global_LC_sum_rate =
            global_LC_sum_rate + g_nvme_fgs[new_flow_group_idx].IOPS_SLO;
        // g_max_outstanding_wr =
        //     MAX_IF_REQUESTS *
        //     (global_LC_sum_wr_rate / (double)global_LC_sum_rate +
        //      0.01);  // hardcoded 1% burst out of 512/max
        g_wr_ratio_limit = 100 * global_LC_sum_wr_rate / global_LC_sum_rate;
        g_be_wr_ratio_limit =
            max(g_wr_ratio_limit - 5, 0);  // hardcoded 5% lower wr ratio

        new_global_token_rate = lookup_device_token_rate(
            g_nvme_fgs[new_flow_group_idx].latency_us_SLO);
        if (new_global_token_rate > global_token_rate) {
            new_global_token_rate =
                global_token_rate;  // keep limit based on strictest latency SLO
        }

#ifndef OVERLOAD_MODE
        if (new_global_LC_sum_token_rate > new_global_token_rate) {
            // control plane notifies tenant can't meet its SLO
            // don't update the global token rate since won't regsiter this
            // tenant
            log_err("CANNOT SATISFY TENANT's SLO: %lu > %lu\n",
                    new_global_LC_sum_token_rate, new_global_token_rate);
            spin_unlock(&nvme_bitmap_lock);
            return -RET_CANTMEETSLO;
        }
#endif
        g_shared_deficit_limit += g_nvme_fgs[new_flow_group_idx].deficit_limit /
                                  TOKEN_DEFICIT_LIMIT_DIV;
        if (global_token_rate != new_global_token_rate) {
            global_token_rate = new_global_token_rate;
            printf("New global token rate: %lu tokens/s.\n", global_token_rate);
        }
        global_LC_sum_token_rate = new_global_LC_sum_token_rate;

        global_num_lc_tenants++;
    } else {
        global_num_best_effort_tenants++;
        global_readonly_flag =
            false;  // assume BE tenant has rd/wr mixed workload
    }

    if (global_num_best_effort_tenants &&
        global_token_rate > global_LC_sum_token_rate) {
        be_token_rate_per_tenant =
            (global_token_rate - global_LC_sum_token_rate) /
            global_num_best_effort_tenants;
        token_rate_per_tenant =
            (global_token_rate - global_LC_sum_token_rate) /
            (global_num_lc_tenants + global_num_best_effort_tenants);
        lc_token_rate_boost_when_no_BE = 0;
    } else {
        be_token_rate_per_tenant = 0;
        if (global_num_lc_tenants)
            lc_token_rate_boost_when_no_BE =
                (global_token_rate - global_LC_sum_token_rate) /
                global_num_lc_tenants;
    }
    atomic_write(&global_be_token_rate_per_tenant, be_token_rate_per_tenant);
    atomic_write(&global_token_rate_per_tenant, token_rate_per_tenant);

    // if number of BE tenants has changes from 0 to 1 or more (or vice versa)
    // adjust LC tenant boost (only want to boost if no BE tenants registered)
    if (lc_token_rate_boost_when_no_BE != global_lc_boost_no_BE) {
        global_lc_boost_no_BE = lc_token_rate_boost_when_no_BE;
        readjust_lc_tenant_token_limits();
    }
    spin_unlock(&nvme_bitmap_lock);

    printf("Global BE token rate per tenant: %u\n",
           atomic_read(&global_be_token_rate_per_tenant));

    return 1;
}

int recalculate_weights_remove(long flow_group_idx) {
    long i;
    unsigned int strictest_latency_SLO = UINT_MAX;
    unsigned int be_token_rate_per_tenant;
    unsigned long lc_token_rate_boost_when_no_BE = 0;

    spin_lock(&nvme_bitmap_lock);

    if (g_nvme_fgs[flow_group_idx].latency_critical_flag) {
        // find new strictest latency SLO
        global_readonly_flag = true;
        for (i = 0; i < MAX_NVME_FLOW_GROUPS; i++) {
            if (bitmap_test(g_nvme_fgs_bitmap, i) && i != flow_group_idx) {
                if (g_nvme_fgs[i].latency_critical_flag) {
                    if (g_nvme_fgs[i].latency_us_SLO < strictest_latency_SLO) {
                        strictest_latency_SLO = g_nvme_fgs[i].latency_us_SLO;
                    }
                    if (g_nvme_fgs[i].rw_ratio_SLO < 100) {
                        global_readonly_flag = false;
                    }
                }
            }
        }
        global_LC_sum_token_rate -=
            g_nvme_fgs[flow_group_idx].scaled_IOPS_limit;
        global_LC_sum_wr_rate -=
            g_nvme_fgs[flow_group_idx].IOPS_SLO *
            (100 - g_nvme_fgs[flow_group_idx].rw_ratio_SLO) / 100;
        global_LC_sum_rate -= g_nvme_fgs[flow_group_idx].IOPS_SLO;
        if (global_LC_sum_rate > 0) {
            // g_max_outstanding_wr =
            //     MAX_IF_REQUESTS *
            //     (global_LC_sum_wr_rate / (double)global_LC_sum_rate +
            //      0.01);  // hardcoded 1% burst
            g_wr_ratio_limit = 100 * global_LC_sum_wr_rate / global_LC_sum_rate;
            g_be_wr_ratio_limit = min(g_wr_ratio_limit + 5, 100);
        }
        global_token_rate = lookup_device_token_rate(strictest_latency_SLO);
        g_shared_deficit_limit -=
            g_nvme_fgs[flow_group_idx].deficit_limit / TOKEN_DEFICIT_LIMIT_DIV;
        // printf("Flow %ld finished %ld requests\n", flow_group_idx,
        // g_nvme_fgs[flow_group_idx].completions); printf("Global token rate:
        // %lu tokens/s\n", global_token_rate);

        global_num_lc_tenants--;
    } else {
        global_num_best_effort_tenants--;
    }

    if (global_num_best_effort_tenants) {
        global_readonly_flag = false;
        be_token_rate_per_tenant =
            (global_token_rate - global_LC_sum_token_rate) /
            global_num_best_effort_tenants;
        lc_token_rate_boost_when_no_BE = 0;
    } else {
        be_token_rate_per_tenant = 0;
        if (global_num_lc_tenants)
            lc_token_rate_boost_when_no_BE =
                (global_token_rate - global_LC_sum_token_rate) /
                global_num_lc_tenants;
    }
    atomic_write(&global_be_token_rate_per_tenant, be_token_rate_per_tenant);

    // if number of BE tenants has changes from 0 to 1 or more (or vice versa)
    // adjust LC tenant boost (only want to boost if no BE tenants registered)
    if (lc_token_rate_boost_when_no_BE != global_lc_boost_no_BE) {
        global_lc_boost_no_BE = lc_token_rate_boost_when_no_BE;
        readjust_lc_tenant_token_limits();
    }

    spin_unlock(&nvme_bitmap_lock);

    return 1;
}

// request cost scales linearly with size above 4KB
// note: may need to adjust this if does not match your Flash device behavior
int nvme_compute_req_cost(int req_type, size_t req_len) {
    if (req_len <= 0) {
        printf("ERROR: request size <= 0!\n");
        return 0;
    }

    int len_scale_factor = 1;

    if (req_len > 4096) {
        // divide req_len by 4096 and round up
        len_scale_factor = (req_len + 4096 - 1) / 4096;
    }

    if (req_type == NVME_CMD_READ) {
        return NVME_READ_COST * len_scale_factor;
    } else if (req_type == NVME_CMD_WRITE) {
        return NVME_WRITE_COST * len_scale_factor;
    }
    return 1;
}

static void sgl_reset_cb(void *cb_arg, uint32_t sgl_offset) {
    struct nvme_ctx *ctx = (struct nvme_ctx *)cb_arg;

    ctx->user_buf.sgl_buf.current_sgl = sgl_offset / SGL_PAGE_SIZE;
}

static int sgl_next_cb(void *cb_arg, uint64_t *address, uint32_t *length) {
    void *paddr;
    void __user *__restrict temp;
    struct nvme_ctx *ctx = (struct nvme_ctx *)cb_arg;

    if (ctx->user_buf.sgl_buf.current_sgl >= ctx->user_buf.sgl_buf.num_sgls) {
        *address = 0;
        *length = 0;
        printf("WARNING: nvme req size mismatch\n");
        assert(0);
    } else {
        temp = ctx->user_buf.sgl_buf.sgl[ctx->user_buf.sgl_buf.current_sgl];
        ctx->user_buf.sgl_buf.current_sgl++;
        *address = (void *)temp;
        *length = 4096;  // PGSIZE_4KB
    }
    return 0;
}

void issue_nvme_req(struct nvme_ctx *ctx) {
    int ret;

    KSTATS_VECTOR(issue_nvme_req);

    // don't schedule request on flash if FAKE_FLASH test
    if (g_nvme_dev_model == FAKE_FLASH) {
        if (ctx->cmd == NVME_CMD_READ) {
            usys_nvme_response(ctx->cookie, ctx->user_buf.buf, RET_OK);
            percpu_get(received_nvme_completions)++;
        } else if (ctx->cmd == NVME_CMD_WRITE) {
            usys_nvme_written(ctx->cookie, RET_OK);
            percpu_get(received_nvme_completions)++;
        }
        free_local_nvme_ctx(ctx);

        return;
    }

    if (ctx->cmd == NVME_CMD_READ) {
        ret = spdk_nvme_ns_cmd_readv(ctx->ns, percpu_get(qpair), ctx->lba,
                                     ctx->lba_count, nvme_read_cb, ctx, 0,
                                     sgl_reset_cb, sgl_next_cb);

    } else if (ctx->cmd == NVME_CMD_WRITE) {
        ret = spdk_nvme_ns_cmd_writev(ctx->ns, percpu_get(qpair), ctx->lba,
                                      ctx->lba_count, nvme_write_cb, ctx, 0,
                                      sgl_reset_cb, sgl_next_cb);
        g_outstanding_wr++;
    } else {
        panic("unrecognized nvme request\n");
    }
    if (ctx->lc_req) g_outstanding_lc_requests++;
    g_outstanding_requests++;
    // dynamic max outstanding writes
    // g_max_outstanding_wr =
    //     g_outstanding_requests *
    //         (global_LC_sum_wr_rate / (double)global_LC_sum_rate + 0.1) +
    //     1;  // at least one
    if (ret < 0) {
        printf("Error submitting nvme request\n");
        printf("Current outstanding: %ld\n", g_outstanding_requests);
        panic("Ran out of NVMe cmd buffer space\n");
    }
}

long bsys_nvme_write(hqu_t priority, void *buf, unsigned long lba,
                     unsigned int lba_count, unsigned long cookie) {
    printf("bsys_nvme_write should not be called\n");
}
long bsys_nvme_read(hqu_t priority, void *buf, unsigned long lba,
                    unsigned int lba_count, unsigned long cookie) {
    printf("bsys_nvme_read should not be called\n");
}

long bsys_nvme_writev(hqu_t fg_handle, void __user **__restrict buf,
                      int num_sgls, unsigned long lba, unsigned int lba_count,
                      unsigned long cookie) {
    struct spdk_nvme_ns *ns;
    struct nvme_ctx *ctx;
    int ret;

    KSTATS_VECTOR(bsys_nvme_writev);

    ns = spdk_nvme_ctrlr_get_ns(nvme_ctrlr[cpu2ssd[percpu_get(cpu_id)]],
                                global_ns_id);

    ctx = alloc_local_nvme_ctx();
    if (ctx == NULL) {
        printf(
            "ERROR: Cannot allocate memory for nvme_ctx in bsys_nvme_read\n");
        return -RET_NOMEM;
    }
    ctx->cookie = cookie;
    ctx->user_buf.sgl_buf.sgl = buf;
    ctx->user_buf.sgl_buf.num_sgls = num_sgls;

    // Store all info in ctx before add to software queue
    ctx->tid = percpu_get(cpu_nr);
    ctx->cmd = NVME_CMD_WRITE;
    ctx->ns = ns;
    ctx->lba = lba;
    ctx->lba_count = lba_count;
    ctx->fg_handle = fg_handle;

    if (!g_nvme_sched_mode) {
#ifdef REFLEX_SCHED
        ret = nvme_sw_queue_push_back(&percpu_get(g_default_swq), ctx);
        if (ret != 0) {
            free_local_nvme_ctx(ctx);
            return -RET_NOMEM;
        }
#else
        ret = nvme_sw_table_push_back(g_nvme_sw_table, 0, ctx);
        if (ret != 0) {
            printf("sw table has %ld requests\n",
                   nvme_sw_table_count(g_nvme_sw_table, 0));
            return -RET_NOMEM;
        }
#endif
    }
#ifdef REFLEX_SCHED
    else if (g_nvme_sched_mode == REFLEX || g_nvme_sched_mode == REFLEX_RR) {
        struct nvme_sw_queue *swq = g_nvme_fgs[fg_handle].nvme_swq;
        ctx->req_cost = nvme_compute_req_cost(
            NVME_CMD_WRITE, lba_count * global_ns_sector_size);
        ret = nvme_sw_queue_push_back(swq, ctx);
        if (ret != 0) {
            free_local_nvme_ctx(ctx);
            return -RET_NOMEM;
        }
    }
#endif
    else {
        ctx->req_cost = nvme_compute_req_cost(
            NVME_CMD_WRITE, lba_count * global_ns_sector_size);
        if (g_nvme_fgs[fg_handle].latency_critical_flag &&
            nvme_sw_table_isempty(g_nvme_sw_table, fg_handle)) {
            nvme_lc_tenant_activate(&percpu_get(tenant_manager), fg_handle);
            ctx->lc_req = true;
        }
        if (!g_nvme_fgs[fg_handle].latency_critical_flag &&
            nvme_sw_table_isempty(g_nvme_sw_table, fg_handle)) {
            nvme_be_tenant_activate(&percpu_get(tenant_manager), fg_handle);
            ctx->lc_req = false;
        }
        ret = nvme_sw_table_push_back(g_nvme_sw_table, fg_handle, ctx);
        d_received_requests++;
        if (ret != 0) {
            print_queue_status();
            free_local_nvme_ctx(ctx);
            return -RET_NOMEM;
        }
    }

    return RET_OK;
}

long bsys_nvme_readv(hqu_t fg_handle, void __user **__restrict buf,
                     int num_sgls, unsigned long lba, unsigned int lba_count,
                     unsigned long cookie) {
    struct spdk_nvme_ns *ns;
    struct nvme_ctx *ctx;
    int ret;

    KSTATS_VECTOR(bsys_nvme_readv);

    ns = spdk_nvme_ctrlr_get_ns(nvme_ctrlr[cpu2ssd[percpu_get(cpu_id)]],
                                global_ns_id);

    ctx = alloc_local_nvme_ctx();
    if (ctx == NULL) {
        printf(
            "ERROR: Cannot allocate memory for nvme_ctx in bsys_nvme_read\n");
        return -RET_NOMEM;
    }
    ctx->cookie = cookie;
    ctx->user_buf.sgl_buf.sgl = buf;
    ctx->user_buf.sgl_buf.num_sgls = num_sgls;

    // Store all info in ctx before add to software queue
    ctx->tid = RTE_PER_LCORE(cpu_nr);
    ctx->cmd = NVME_CMD_READ;
    ctx->ns = ns;
    ctx->lba = lba;
    ctx->lba_count = lba_count;
    ctx->fg_handle = fg_handle;

    if (!g_nvme_sched_mode) {
#ifdef REFLEX_SCHED
        ret = nvme_sw_queue_push_back(&percpu_get(g_default_swq), ctx);
        if (ret != 0) {
            free_local_nvme_ctx(ctx);
            return -RET_NOMEM;
        }
#else
        ret = nvme_sw_table_push_back(g_nvme_sw_table, 0, ctx);
        if (ret != 0) {
            printf("sw table has %ld requests\n",
                   nvme_sw_table_count(g_nvme_sw_table, 0));
            return -RET_NOMEM;
        }
#endif
    }
#ifdef REFLEX_SCHED
    else if (g_nvme_sched_mode == REFLEX || g_nvme_sched_mode == REFLEX_RR) {
        struct nvme_sw_queue *swq = g_nvme_fgs[fg_handle].nvme_swq;
        ctx->req_cost = nvme_compute_req_cost(
            NVME_CMD_READ, lba_count * global_ns_sector_size);
        ret = nvme_sw_queue_push_back(swq, ctx);
        if (ret != 0) {
            free_local_nvme_ctx(ctx);
            return -RET_NOMEM;
        }
    }
#endif
    else {
        ctx->req_cost = nvme_compute_req_cost(
            NVME_CMD_READ, lba_count * global_ns_sector_size);
        if (g_nvme_fgs[fg_handle].latency_critical_flag &&
            nvme_sw_table_isempty(g_nvme_sw_table, fg_handle)) {
            nvme_lc_tenant_activate(&percpu_get(tenant_manager), fg_handle);
            // printf("LC tenant %ld activated\n", fg_handle);
            int active_count =
                (percpu_get(tenant_manager).lc_tail -
                 percpu_get(tenant_manager).lc_head + MAX_NVME_FLOW_GROUPS) %
                MAX_NVME_FLOW_GROUPS;
            // printf("Tenant manager has %d active LC tenants\n",
            // active_count);
        }
        if (!g_nvme_fgs[fg_handle].latency_critical_flag &&
            nvme_sw_table_isempty(g_nvme_sw_table, fg_handle)) {
            nvme_be_tenant_activate(&percpu_get(tenant_manager), fg_handle);
        }
        // if (nvme_lc_tenant_isactivated(&percpu_get(tenant_manager),
        //                                fg_handle) == false) {
        //     printf("ERROR: LC tenant %ld is not activated\n", fg_handle);
        //     return RET_FAULT;
        // }
        ret = nvme_sw_table_push_back(g_nvme_sw_table, fg_handle, ctx);
        d_received_requests++;
        if (ret != 0) {
            print_queue_status();
            free_local_nvme_ctx(ctx);
            return RET_NOMEM;
        }
    }

    return RET_OK;
}

unsigned long try_acquire_global_tokens(unsigned long token_demand) {
    unsigned long new_token_level = 0;
    unsigned long avail_tokens = 0;

    while (1) {
        avail_tokens = atomic_u64_read(&global_leftover_tokens);

        if (token_demand > avail_tokens) {
            if (atomic_u64_cmpxchg(&global_leftover_tokens, avail_tokens, 0)) {
                return avail_tokens;
            }
        } else {
            new_token_level = avail_tokens - token_demand;
            if (atomic_u64_cmpxchg(&global_leftover_tokens, avail_tokens,
                                   new_token_level)) {
                return token_demand;
            }
        }
    }
}

// inline int try_loan_global_tokens(long token_credit, long token_demand) {
//     unsigned long new_debt_level = 0;
//     unsigned long avail_tokens = 0;

//     if (token_credit >= token_demand) return 0;

//     new_debt_level = global_token_debt + token_demand;
//     avail_tokens = atomic_u64_read(&global_leftover_tokens);
//     if (new_debt_level > avail_tokens) {
//         return 1; // Failed to loan
//     }
//     global_token_debt = new_debt_level;
//     return 0;
// }

void nvme_process_completions() {
    int i;
    int max_completions = 4096;
    int this_completions;

    if (CFG.num_nvmedev == 0 || CFG.ns_sizes[0] != 0) return;

    for (i = 0; i < percpu_get(open_ev_ptr); i++) {
        usys_nvme_opened(percpu_get(open_ev[i]), global_ns_size,
                         global_ns_sector_size);
        percpu_get(received_nvme_completions)++;
    }
    percpu_get(open_ev_ptr) = 0;
    this_completions =
        spdk_nvme_qpair_process_completions(percpu_get(qpair), max_completions);
    g_outstanding_requests -= this_completions;
    percpu_get(received_nvme_completions) += this_completions;
}
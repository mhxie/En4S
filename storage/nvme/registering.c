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

#include <ix/cfg.h>
#include <ix/kstats.h>
#include <nvme/nvmedev.h>
#include <nvme/tenant_mgmt.h>
#ifdef REFLEX_SCHED
#include <nvme/sw_queue.h>
#endif

#define SLO_REQ_SIZE 4096

DECLARE_BITMAP(g_nvme_fgs_bitmap, MAX_NVME_FLOW_GROUPS);
extern struct nvme_flow_group g_nvme_fgs[MAX_NVME_FLOW_GROUPS];
extern int join_flow_start;
extern int join_flow_end;
extern int leave_flow_start;
extern int leave_flow_end;
extern struct nvme_flow_group_min pending_join_flows[MAX_NVME_FLOW_GROUPS];
extern long pending_leave_flows[MAX_NVME_FLOW_GROUPS];
RTE_DECLARE_PER_LCORE(unsigned long, last_sys_bpoll_time);
RTE_DECLARE_PER_LCORE(double, flow_credit);

RTE_DECLARE_PER_LCORE(struct en4s_tenant_mgmt, tenant_manager);
RTE_DECLARE_PER_LCORE(unsigned long, min_scaled_IOPS);
RTE_DECLARE_PER_LCORE(unsigned long, min_tenant_count);
#ifdef REFLEX_SCHED
RTE_DECLARE_PER_LCORE(unsigned long, roundrobin_start);
RTE_DECLARE_PER_LCORE(struct reflex_tenant_mgmt, reflex_tenant_manager);
RTE_DEFINE_PER_LCORE(struct mempool,
                     nvme_swq_mempool __attribute__((aligned(64))));

extern struct mempool_datastore nvme_swq_datastore;

int init_local_nvme_swq_mempool(void) {
    int ret;
    struct mempool *m3 = &percpu_get(nvme_swq_mempool);
    ret = mempool_create(m3, &nvme_swq_datastore, MEMPOOL_SANITY_PERCPU,
                         percpu_get(cpu_id));
    return ret;
}

struct nvme_sw_queue *alloc_local_nvme_swq(void) {
    return mempool_alloc(&percpu_get(nvme_swq_mempool));
}

void free_local_nvme_swq(struct nvme_sw_queue *q) {
    mempool_free(&percpu_get(nvme_swq_mempool), q);
}
#endif

unsigned long scaled_IOPS(unsigned long IOPS, int rw_ratio_100) {
    double scaledIOPS;
    double rw_ratio = (double)rw_ratio_100 / (double)100;

    /*
     * NOTE: when calculating token reservation for latency-critical tenants,
     * 		 assume SLO specificed for 4kB requests
     * 		 e.g. if your application's IOPS SLO is 100K IOPS for 8K IOs,
     * 		      register your app's SLO with ReFlex as 200K IOPS
     */
    scaledIOPS =
        (IOPS * rw_ratio * nvme_compute_req_cost(NVME_CMD_READ, SLO_REQ_SIZE)) +
        (IOPS * (1 - rw_ratio) *
         nvme_compute_req_cost(NVME_CMD_WRITE, SLO_REQ_SIZE));
    return (unsigned long)(scaledIOPS + 0.5);
}

int set_nvme_flow_group_id(long flow_group_id, long *fg_handle_to_set,
                           unsigned long cookie) {
    int i;
    int next_avail = 0;
    // first check if already registered this flow
    spin_lock(&nvme_bitmap_lock);
    // starting from 1, 0 is reserved for default flow group
    for (i = 1; i < MAX_NVME_FLOW_GROUPS; i++) {
        if (bitmap_test(g_nvme_fgs_bitmap, i)) {
            // if already registered this flow group, return its index
            if (g_nvme_fgs[i].flow_group_id == flow_group_id &&
                g_nvme_fgs[i].tid == RTE_PER_LCORE(cpu_nr)) {
                *fg_handle_to_set = i;
                spin_unlock(&nvme_bitmap_lock);
                // if (g_nvme_fgs[i].cookie == cookie)
                return 1;
                // else
                //     return 2;
            }
        } else {
            if (next_avail == 0) {
                next_avail = i;
            }
        }
    }

    if (next_avail == MAX_NVME_FLOW_GROUPS) {
        spin_unlock(&nvme_bitmap_lock);
        return -ENOMEM;
    }

    bitmap_set(g_nvme_fgs_bitmap, next_avail);
    spin_unlock(&nvme_bitmap_lock);

    *fg_handle_to_set = next_avail;
    return 0;
}

long bsys_nvme_register_flow(long flow_group_id, unsigned long cookie,
                             unsigned int latency_us_SLO, unsigned int IOPS_SLO,
                             unsigned short rw_ratio_SLO) {
    long fg_handle = 0;
    struct nvme_flow_group *nvme_fg;
    int ret = 0;
    int flow_state = 0;
    int lc_tenant_count = 0;
    struct en4s_tenant_mgmt *thread_tenant_manager;
#ifdef REFLEX_SCHED
    struct reflex_tenant_mgmt *thread_tenant_manager_old;
    struct nvme_sw_queue *swq;
#endif
    KSTATS_VECTOR(bsys_nvme_register_flow);

    if (!g_nvme_sched_mode) {
        printf(
            "Register new-tenant %ld (flow_group: %ld). Managed by thread "
            "%ld, scheduler=off\n",
            fg_handle, flow_group_id, RTE_PER_LCORE(cpu_nr));
        nvme_fg = &g_nvme_fgs[0];
        nvme_fg->conn_ref_count++;

        usys_nvme_registered_flow(0, cookie, RET_OK);
        return RET_OK;
    }

    flow_state = set_nvme_flow_group_id(flow_group_id, &fg_handle, cookie);
    if (fg_handle < 0) {
        log_err("error: exceeded max (%d) nvme flow groups!\n",
                MAX_NVME_FLOW_GROUPS);
    }

    nvme_fg = &g_nvme_fgs[fg_handle];
    nvme_fg->completions = 0;

    if (latency_us_SLO == 0 || IOPS_SLO == 0) {
        nvme_fg->latency_critical_flag = false;
    } else {
        nvme_fg->latency_critical_flag = true;
    }

    if (flow_state == 2) {
        // Old connection with new SLO
        bsys_nvme_unregister_flow(fg_handle);
        printf(
            "WARNING: tenant connection registered different SLO, will "
            "overwrite previous SLO for all of this tenant's connections. "
            "supporting only 1 SLO per tenant.\n");
        flow_state = 0;
    } else {
        nvme_fg->scaled_IOPS_limit = scaled_IOPS(IOPS_SLO, rw_ratio_SLO);
        nvme_fg->smooth_share =
            nvme_fg->scaled_IOPS_limit / SMOOTHING_FACTOR / NVME_READ_COST;
        nvme_fg->deficit_limit =
            nvme_fg->scaled_IOPS_limit * TOKEN_DEFICIT_LIMIT_PERC / 100;
        if (nvme_fg->smooth_share == 0) nvme_fg->smooth_share = 1;
        nvme_fg->scaled_IOPuS_limit = nvme_fg->scaled_IOPS_limit / (double)1E6;
#ifdef SCHED_DEBUG
        printf("scaled_IOPS_limit: %lu\n", nvme_fg->scaled_IOPS_limit);
        printf("smooth_share: %u\n", nvme_fg->smooth_share);
#endif
    }
    if (flow_state == 1) {
        // New connection with old SLO
        ret = recalculate_weights_add(fg_handle);
        if (ret < 0) {
            printf("WARNING: cannot satisfy SLO\n");
            return -RET_CANTMEETSLO;
        }
    } else if (flow_state == 0) {
        // New connection with new SLO
        nvme_fg->flow_group_id = flow_group_id;
        // nvme_fg->cookie = cookie;
        nvme_fg->latency_us_SLO = latency_us_SLO;
        nvme_fg->IOPS_SLO = IOPS_SLO;
        nvme_fg->rw_ratio_SLO = rw_ratio_SLO;
        nvme_fg->tid = RTE_PER_LCORE(cpu_nr);

        ret = recalculate_weights_add(fg_handle);
        if (ret < 0) {
            printf("WARNING: cannot satisfy SLO\n");
            return -RET_CANTMEETSLO;
        }

        thread_tenant_manager = &percpu_get(tenant_manager);

        nvme_fg->conn_ref_count = 0;
        if (latency_us_SLO == 0 || IOPS_SLO == 0) {
            thread_tenant_manager->num_be_tenants++;
        } else {
            thread_tenant_manager->num_lc_tenants++;
            if (thread_tenant_manager->num_lc_tenants == 1) {
                percpu_get(min_scaled_IOPS) = nvme_fg->scaled_IOPS_limit;
                percpu_get(min_tenant_count) = 1;
            } else if (thread_tenant_manager->num_lc_tenants > 1) {
                if (percpu_get(min_scaled_IOPS) > nvme_fg->scaled_IOPS_limit) {
                    percpu_get(min_scaled_IOPS) = nvme_fg->scaled_IOPS_limit;
                    percpu_get(min_tenant_count) = 1;
                } else if (percpu_get(min_scaled_IOPS) ==
                           nvme_fg->scaled_IOPS_limit) {
                    percpu_get(min_tenant_count) += 1;
                }
            }
        }
#ifdef REFLEX_SCHED
        swq = alloc_local_nvme_swq();
        if (swq == NULL) {
            log_err("error: can't allocate nvme_swq for flow group\n");
            return -RET_NOMEM;
        }
        nvme_fg->nvme_swq = swq;
        nvme_sw_queue_init(swq, fg_handle);
        // printf("swq %lx inited to fg_handle: %ld.\n", nvme_fg->nvme_swq,
        // fg_handle);
        thread_tenant_manager_old = &percpu_get(reflex_tenant_manager);
        list_add(&thread_tenant_manager_old->tenant_swq_head, &swq->link);
        thread_tenant_manager_old->num_tenants++;
        if (latency_us_SLO == 0 || IOPS_SLO == 0) {
            thread_tenant_manager_old->num_best_effort_tenants++;
        }
        percpu_get(roundrobin_start) = 0;
#endif
    }
#ifdef SCHED_DEBUG
    printf(
        "New connection registered, scaled_IOPS_limit: %lu, smooth_share: %u, "
        "deficit_limit: %u\n ",
        nvme_fg->scaled_IOPS_limit, nvme_fg->smooth_share,
        nvme_fg->deficit_limit);
#endif
    nvme_fg->conn_ref_count++;
    nvme_fg->last_sched_time = rdtsc();
    usys_nvme_registered_flow(fg_handle, cookie, RET_OK);

    return RET_OK;
}

long bsys_nvme_unregister_flow(long fg_handle) {
    struct en4s_tenant_mgmt *thread_tenant_manager;
#ifdef REFLEX_SCHED
    struct reflex_tenant_mgmt *thread_tenant_manager_old;
#endif
    struct nvme_flow_group *nvme_fg;
    unsigned long smallest_IOPS_limit = ULONG_MAX;
    int i = 0;
    KSTATS_VECTOR(bsys_nvme_unregister_flow);

    if (!g_nvme_sched_mode) {
        nvme_fg = &g_nvme_fgs[0];
        nvme_fg->conn_ref_count--;
        printf("Flow deregistered, remaining %d connections\n",
               nvme_fg->conn_ref_count);
        usys_nvme_unregistered_flow(0, RET_OK);
        return RET_OK;
    }
    nvme_fg = &g_nvme_fgs[fg_handle];
    nvme_fg->conn_ref_count--;
    nvme_fg->scaled_IOPS_limit -=
        scaled_IOPS(nvme_fg->IOPS_SLO, nvme_fg->rw_ratio_SLO) / (double)1E6;
    if (nvme_fg->scaled_IOPS_limit < 0) {
        printf("Unexpected unregisteration (handle: %ld)\n", fg_handle);
    }

    recalculate_weights_remove(fg_handle);

    if (nvme_fg->conn_ref_count == 0) {
#ifdef REFLEX_SCHED
        thread_tenant_manager_old = &percpu_get(reflex_tenant_manager);
        list_del(&g_nvme_fgs[fg_handle].nvme_swq->link);
        free_local_nvme_swq(g_nvme_fgs[fg_handle].nvme_swq);
        thread_tenant_manager_old->num_tenants--;
#endif
        thread_tenant_manager = &percpu_get(tenant_manager);
        if (!nvme_fg->latency_critical_flag) {
            thread_tenant_manager->num_be_tenants--;
        } else {
            thread_tenant_manager->num_lc_tenants--;
            if (thread_tenant_manager->num_lc_tenants == 1) {
                percpu_get(min_scaled_IOPS) = smallest_IOPS_limit;
                percpu_get(min_tenant_count) = 0;
            } else {
                if (percpu_get(min_scaled_IOPS) == nvme_fg->scaled_IOPS_limit) {
                    // fine new min, FIXME: use minheap to improve efficiency
                    percpu_get(min_tenant_count) -= 1;
                    if (percpu_get(min_tenant_count) == 0) {
                        for (i = 0; i < MAX_NVME_FLOW_GROUPS; i++) {
                            if (bitmap_test(g_nvme_fgs_bitmap, i) &&
                                i != fg_handle) {
                                if (g_nvme_fgs[i].latency_critical_flag) {
                                    if (g_nvme_fgs[i].scaled_IOPS_limit <
                                        smallest_IOPS_limit) {
                                        percpu_get(min_scaled_IOPS) =
                                            g_nvme_fgs[i].scaled_IOPS_limit;
                                        percpu_get(min_tenant_count) = 1;
                                    } else if (g_nvme_fgs[i]
                                                   .scaled_IOPS_limit ==
                                               smallest_IOPS_limit) {
                                        percpu_get(min_tenant_count) += 1;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        spin_lock(&nvme_bitmap_lock);
        bitmap_clear(g_nvme_fgs_bitmap, fg_handle);
        spin_unlock(&nvme_bitmap_lock);
    }

    usys_nvme_unregistered_flow(fg_handle, RET_OK);

    return RET_OK;
}

long bsys_nvme_register_flow_fast(long flow_group_id, unsigned long cookie,
                                  unsigned int latency_us_SLO,
                                  unsigned int IOPS_SLO,
                                  unsigned short rw_ratio_SLO) {
    struct nvme_flow_group *nvme_fg;
    pending_join_flows[join_flow_start].cookie = cookie;
    pending_join_flows[join_flow_start].latency_us_SLO = latency_us_SLO;
    pending_join_flows[join_flow_start].IOPS_SLO = IOPS_SLO;
    pending_join_flows[join_flow_start].rw_ratio_SLO = rw_ratio_SLO;
    nvme_fg = &g_nvme_fgs[0];
    nvme_fg->conn_ref_count++;

    usys_nvme_registered_flow(0, cookie, RET_OK);
    return RET_OK;
}

long bsys_nvme_unregister_flow_fast(long fg_handle) {
    struct nvme_flow_group *nvme_fg;
    pending_leave_flows[leave_flow_start] = fg_handle;
    leave_flow_start = (leave_flow_start + 1) % MAX_NVME_FLOW_GROUPS;
    nvme_fg = &g_nvme_fgs[0];
    nvme_fg->conn_ref_count--;
    printf("Flow deregistered, remaining %d connections\n",
           nvme_fg->conn_ref_count);
    usys_nvme_unregistered_flow(0, RET_OK);
    return RET_OK;
}

int acquire_flow_operations() {
    // acquire once per sys_bpoll
    unsigned long now = rdtsc();
    unsigned long cycle_delta = now - percpu_get(last_sys_bpoll_time);
    percpu_get(last_sys_bpoll_time) = now;
    percpu_get(flow_credit) +=
        (double)cycle_delta * FLOW_OPPS / rte_get_timer_hz();
    if (percpu_get(flow_credit) > MAX_FLOW_OP) {
        percpu_get(flow_credit) = MAX_FLOW_OP;
    }
    return percpu_get(flow_credit);
}

void release_flow_operations(int used_credit) {
    percpu_get(flow_credit) -= used_credit;
}

inline int slow_register_flows(int num_flows) {
    if (join_flow_start == join_flow_end) {
        return num_flows;
    }
    int flow_end = join_flow_end;
    int fid;
    int remaining_flows = num_flows;
    if (join_flow_start > join_flow_end) {
        flow_end += MAX_NVME_FLOW_GROUPS;
    }
    for (int i = 0; i < num_flows; i++) {
        if (join_flow_start >= flow_end) {
            break;
        }
        fid = (join_flow_start + i) % MAX_NVME_FLOW_GROUPS;
        bsys_nvme_register_flow(pending_join_flows[fid].flow_group_id,
                                pending_join_flows[fid].cookie,
                                pending_join_flows[fid].latency_us_SLO,
                                pending_join_flows[fid].IOPS_SLO,
                                pending_join_flows[fid].rw_ratio_SLO);
        join_flow_start++;
        remaining_flows--;
    }
    if (join_flow_start >= MAX_NVME_FLOW_GROUPS) {
        join_flow_start -= MAX_NVME_FLOW_GROUPS;
    }
    return remaining_flows;
}

int slow_deregister_flows(int num_flows) {
    if (leave_flow_start == leave_flow_end) {
        return num_flows;
    }
    int flow_end = leave_flow_end;
    int fid;
    int remaining_flows = num_flows;
    if (leave_flow_start > leave_flow_end) {
        flow_end += MAX_NVME_FLOW_GROUPS;
    }
    // while (leave_flow_start < flow_end) {
    //     if (remaining_flows <= 0) break;
    //     fid = pending_leave_flows[i];
    //     bsys_nvme_unregister_flow(fid);
    //     leave_flow_start++;
    //     remaining_flows--;
    // }
    for (int i = 0; i < num_flows; i++) {
        if (leave_flow_start >= flow_end) break;
        fid = (leave_flow_start + i) % MAX_NVME_FLOW_GROUPS;
        bsys_nvme_unregister_flow(pending_leave_flows[fid]);
        leave_flow_start++;
        remaining_flows--;
    }
    if (leave_flow_start >= MAX_NVME_FLOW_GROUPS) {
        leave_flow_start -= MAX_NVME_FLOW_GROUPS;
    }
    return remaining_flows;
}
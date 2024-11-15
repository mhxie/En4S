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

/*
 * Tenant management for NVMe devices
 */

#pragma once
#include <ix/bitmap.h>

#define MAX_NVME_FLOW_GROUPS 4096
extern DEFINE_BITMAP(g_nvme_fgs_bitmap, MAX_NVME_FLOW_GROUPS);

// #define iterate_active_tenants_by_type(m, type)               \
//     for (long i = m->type##_head;                             \
//          (m->type##_head <= m->type##_tail                    \
//               ? (i < m->type##_tail)                          \
//               : (i < m->type##_tail + MAX_NVME_FLOW_GROUPS)); \
//          i++)
#define iterate_active_tenants_by_type(m, type)             \
    for (long i = m->type##_head;                           \
         m->type##_head <= m->type##_tail                   \
             ? (i < m->type##_tail)                         \
             : (i < m->type##_tail + MAX_NVME_FLOW_GROUPS); \
         i++)

#define iterate_all_tenants(fg_handle) \
    for (fg_handle = 0; fg_handle < MAX_NVME_FLOW_GROUPS; fg_handle++)

struct en4s_tenant_mgmt {
    long active_lc_tenants[MAX_NVME_FLOW_GROUPS];
    long active_be_tenants[MAX_NVME_FLOW_GROUPS];
    long active_lc_tenants_requeue[MAX_NVME_FLOW_GROUPS];
    long active_be_tenants_requeue[MAX_NVME_FLOW_GROUPS];
    uint16_t lc_head;
    uint16_t lc_tail;
    uint16_t be_head;
    uint16_t be_tail;
    uint16_t num_lc_tenants;
    uint16_t num_be_tenants;
    uint16_t num_lc_requeue_tenants;
    uint16_t num_be_requeue_tenants;
};

struct en4s_tenant {
    // struct list_node link;
    long fg_handle;
    uint32_t queue_head;
    uint32_t queue_tail;
    uint16_t queue_overflow_count;
    int32_t token_credit;
    uint32_t total_token_demand;
    uint32_t saved_tokens;
    float smoothy_share;
};

inline void init_en4s_tenant_mgmt(struct en4s_tenant_mgmt *manager) {
    int i;
    for (i = 0; i < MAX_NVME_FLOW_GROUPS; i++) {
        manager->active_lc_tenants[i] = -1;
        manager->active_be_tenants[i] = -1;
    }
    manager->lc_head = 0;
    manager->lc_tail = 0;
    manager->be_head = 0;
    manager->be_tail = 0;
    manager->num_lc_tenants = 0;
    manager->num_be_tenants = 0;
}

inline bool nvme_lc_tenant_isempty(struct en4s_tenant_mgmt *manager) {
    return manager->lc_head == manager->lc_tail;
}

inline uint16_t nvme_lc_active_tenant_count(struct en4s_tenant_mgmt *manager) {
    return (manager->lc_tail - manager->lc_head + MAX_NVME_FLOW_GROUPS) %
           MAX_NVME_FLOW_GROUPS;
}

inline bool nvme_lc_tenant_isactivated(struct en4s_tenant_mgmt *manager,
                                       long tenant_id) {
    iterate_active_tenants_by_type(manager, lc) {
        if (manager->active_lc_tenants[i % MAX_NVME_FLOW_GROUPS] == tenant_id) {
            return true;
        }
    }
    return false;
}

inline bool nvme_be_tenant_isactivated(struct en4s_tenant_mgmt *manager,
                                       long tenant_id) {
    iterate_active_tenants_by_type(manager, be) {
        if (manager->active_be_tenants[i % MAX_NVME_FLOW_GROUPS] == tenant_id) {
            return true;
        }
    }
    return false;
}

inline void nvme_lc_tenant_activate(struct en4s_tenant_mgmt *manager,
                                    long tenant_id) {
    manager->active_lc_tenants[manager->lc_tail] = tenant_id;
    manager->lc_tail = (manager->lc_tail + 1) % MAX_NVME_FLOW_GROUPS;
    if (unlikely(manager->lc_tail == manager->lc_head)) {
        printf("Latency-critical tenants exceeds limits\n");
    }
}

inline void nvme_lc_tenant_requeue(struct en4s_tenant_mgmt *manager,
                                   long tenant_id) {
    manager->active_lc_tenants_requeue[manager->num_lc_requeue_tenants] =
        tenant_id;
    manager->num_lc_requeue_tenants++;
}

inline void nvme_lc_tenant_deactivate(struct en4s_tenant_mgmt *manager,
                                      uint32_t count) {
    if (count == 0) {
        return;
    } else {
        // printf("%ld LC tenants deactivated\n", count);
        // int initial_active_count =
        //     (manager->lc_tail - manager->lc_head + MAX_NVME_FLOW_GROUPS) %
        //     MAX_NVME_FLOW_GROUPS;
        // int after_active_count = initial_active_count - count;
        // printf("Tenant manager active LC tenants reduces from %d to %d\n",
        //        initial_active_count, after_active_count);
    }
    manager->lc_head = (manager->lc_head + count) % MAX_NVME_FLOW_GROUPS;
    if (manager->num_lc_requeue_tenants) {
        for (int i = 0; i < manager->num_lc_requeue_tenants; i++) {
            manager->active_lc_tenants[manager->lc_tail] =
                manager->active_lc_tenants_requeue[i];
            manager->lc_tail = (manager->lc_tail + 1) % MAX_NVME_FLOW_GROUPS;
        }
    }
    manager->num_lc_requeue_tenants = 0;
}

inline bool nvme_be_tenant_isempty(struct en4s_tenant_mgmt *manager) {
    return manager->be_head == manager->be_tail;
}

inline uint16_t nvme_be_active_tenant_count(struct en4s_tenant_mgmt *manager) {
    return (manager->be_tail - manager->be_head + MAX_NVME_FLOW_GROUPS) %
           MAX_NVME_FLOW_GROUPS;
}

inline void nvme_be_tenant_activate(struct en4s_tenant_mgmt *manager,
                                    long tenant_id) {
    manager->active_be_tenants[manager->be_tail] = tenant_id;
    manager->be_tail = (manager->be_tail + 1) % MAX_NVME_FLOW_GROUPS;
    if (unlikely(manager->be_tail == manager->be_head)) {
        printf("Best-effort tenants exceeds limits\n");
    }
}

inline void nvme_be_tenant_requeue(struct en4s_tenant_mgmt *manager,
                                   long tenant_id) {
    manager->active_be_tenants_requeue[manager->num_be_requeue_tenants] =
        tenant_id;
    manager->num_be_requeue_tenants++;
}

inline void nvme_be_tenant_deactivate(struct en4s_tenant_mgmt *manager,
                                      uint32_t count) {
    if (count == 0) {
        return;
    } else if (count > 0) {
        // printf("%ld BE tenants deactivated\n", count);
        // printf("Tenant manager active LC tenants reduces from %ld to %ld\n",
        //        manager->be_tail - manager->be_head,
        //        manager->be_tail - manager->be_head -
        //            count);  // no mod, just for debugging
    }
    manager->be_head = (manager->be_head + count) % MAX_NVME_FLOW_GROUPS;
    if (manager->num_be_requeue_tenants) {
        for (int i = 0; i < manager->num_be_requeue_tenants; i++) {
            manager->active_be_tenants[manager->be_tail] =
                manager->active_be_tenants_requeue[i];
            manager->be_tail = (manager->be_tail + 1) % MAX_NVME_FLOW_GROUPS;
        }
    }
    manager->num_be_requeue_tenants = 0;
}
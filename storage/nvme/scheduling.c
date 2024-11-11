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
#include <nvme/nvmedev.h>
#include <nvme/sw_counter.h>
#include <nvme/sw_queue.h>
#include <nvme/sw_table.h>
#include <nvme/tenant_mgmt.h>

// #define NO_SCHED
#define CYCLIC_LIST
#define SINGLE_THREADED

#define MAX_NUM_THREADS 24
static int scheduled_bit_vector[MAX_NUM_THREADS];
extern struct nvme_flow_group g_nvme_fgs[MAX_NVME_FLOW_GROUPS];
extern atomic_u64_t global_leftover_tokens;
extern atomic_t global_be_token_rate_per_tenant;
extern atomic_t global_token_rate_per_tenant;
extern unsigned long global_LC_sum_token_rate;
extern long g_outstanding_requests;
extern long g_outstanding_lc_requests;
extern long g_max_outstanding_wr;
extern int g_wr_ratio_limit;
extern int g_be_wr_ratio_limit;
extern long g_outstanding_wr;
extern long g_shared_deficit_limit;
extern unsigned long g_sched_interval;
extern unsigned long d_received_requests;
extern unsigned long d_last_received_requests;
extern unsigned long long d_accumulated_tokens;
extern double d_accumulated_delta;
extern unsigned long d_accumulated_cost;
extern unsigned long d_donation_count;
extern struct nvme_sw_table *g_nvme_sw_table;

// LC tenants get how many times more opportunities to increment tokens and
// process
unsigned int interation_ratio = 3;
static unsigned long max_loan_times = 0;
#ifdef REFLEX_SCHED
RTE_DECLARE_PER_LCORE(struct reflex_tenant_mgmt, reflex_tenant_manager);
RTE_DECLARE_PER_LCORE(struct nvme_sw_queue, g_default_swq);
RTE_DECLARE_PER_LCORE(unsigned long, last_sched_us);
RTE_DECLARE_PER_LCORE(unsigned long, last_sched_us_be);
RTE_DEFINE_PER_LCORE(int, lc_roundrobin_start);
RTE_DEFINE_PER_LCORE(int, roundrobin_start);
#endif
RTE_DECLARE_PER_LCORE(unsigned long, last_sched_cycles_lc);
RTE_DECLARE_PER_LCORE(unsigned long, last_sched_cycles_be);
RTE_DECLARE_PER_LCORE(unsigned long, g_sched_time);

RTE_DECLARE_PER_LCORE(struct en4s_tenant_mgmt, tenant_manager);
RTE_DECLARE_PER_LCORE(struct sw_counter, rw_counter);
RTE_DECLARE_PER_LCORE(unsigned long, local_leftover_tokens);
RTE_DECLARE_PER_LCORE(unsigned long, local_extra_demand);
RTE_DEFINE_PER_LCORE(unsigned long, min_scaled_IOPS);
RTE_DEFINE_PER_LCORE(unsigned int, min_tenant_count);

/*
 * update_scheduler_bitvector:
 * 		- synchronizes clearing of the global token bucket to limit
 * global BE token accumulation
 * 		- mark global bitvector to indicate this thread has completed a
 * scheduling round
 * 		- if last thread to complete a round, clear the global vector
 * 		- updates to global vector are not atomic operations because
 * want to limit perf overhead and the exact timing of token bucket reset is
 * not critical, as long as we reset approximately after each thread has had
 * a chance to get tokens
 */
static void update_scheduled_bitvector(void) {
    int i;
    scheduled_bit_vector[RTE_PER_LCORE(cpu_nr)]++;

    for (i = 0; i < cpus_active; i++) {
        if (scheduled_bit_vector[i] == 0) break;
    }
    if (i == cpus_active) {  // all other threads scheduled at least once
#ifdef SCHED_DEBUG
        if (atomic_u64_read(&global_leftover_tokens) > 0) {
            printf("Cleared %ld leftover tokens\n",
                   atomic_u64_read(&global_leftover_tokens));
        }
#endif
        atomic_u64_write(&global_leftover_tokens, 0);

        // clear scheduled bit vector
        for (i = 0; i < cpus_active; i++) {
            scheduled_bit_vector[i] = 0;
        }
    }
}

#ifdef REFLEX_SCHED
/*
 * nvme_sched_subround1: schedule latency critical tenant traffic
 */
static inline int nvme_sched_subround1(void) {
    struct reflex_tenant_mgmt *thread_tenant_manager_old;
    struct nvme_sw_queue *swq;
    struct nvme_ctx *ctx;
    unsigned long now;
    unsigned long time_delta;
    long POS_LIMIT = 0;
    unsigned long local_leftover = 0;
    unsigned long local_demand = 0;
    double token_increment;

    now = rdtsc() / (rte_get_timer_hz() / 1e6);  // in us
    time_delta = now - percpu_get(last_sched_us);
    percpu_get(last_sched_us) = now;

    thread_tenant_manager_old = &percpu_get(reflex_tenant_manager);

    list_for_each(&thread_tenant_manager_old->tenant_swq_head, swq, link) {
        // serve latency-critical (LC) tenants
        if (g_nvme_fgs[swq->fg_handle].latency_critical_flag) {
            token_increment =
                (g_nvme_fgs[swq->fg_handle].scaled_IOPuS_limit * time_delta) +
                0.5;  // 0.5 is for rounding
            swq->token_credit += (long)token_increment;
            swq->history_credit += (long)token_increment;
            while ((nvme_sw_queue_isempty(swq) == 0) &&
                   swq->token_credit >
                       -g_nvme_fgs[swq->fg_handle].deficit_limit) {
                if (g_outstanding_requests >= MAX_IF_REQUESTS) {
                    // printf("Temporal bursts! outstanding = %ld\n",
                    // g_outstanding_requests);
                    break;
                }
                nvme_sw_queue_pop_front(swq, &ctx);
                issue_nvme_req(ctx);
                swq->token_credit -= ctx->req_cost;
            }

            /*
             * POS_LIMIT can be tuned to balance work-conservation and favoring
             *of LC traffic
             *	  * default POS_LIMIT    = 3 * token_increment
             *	  						if LC tenant
             *doesn't use tokens accumulated from ~3 sched rounds, donate them
             *
             *   * lower POS_LIMIT 		is good for work-conservation
             *   						(give tokens to
             *BE tenants more easily)
             *
             *   * higher POS_LIMIT 	allows latency-critical tenants to
             *accumulate more tokens & burst
             */
            POS_LIMIT = POS_LIMIT_FACTOR * token_increment;
            if (swq->token_credit > POS_LIMIT) {
                // #ifdef SCHED_DEBUG
                //                 printf("Before donation: swq->token_credit
                //                 %ld\n",
                //                        swq->token_credit);
                // #endif
                local_leftover += (swq->token_credit * TOKEN_FRAC_GIVEAWAY);
                swq->token_credit -= swq->token_credit * TOKEN_FRAC_GIVEAWAY;
                // #ifdef SCHED_DEBUG
                //                 printf("After donation: swq->token_credit
                //                 %ld\n",
                //                        swq->token_credit);
                // #endif
            }
        } else {  // track demand of best-effort (will need for subround2)
            local_demand += swq->total_token_demand - swq->saved_tokens;
        }
    }

    percpu_get(local_extra_demand) = local_demand;
    percpu_get(local_leftover_tokens) = local_leftover;

    return 0;
}

/*
 * nvme_sched_rr_subround1: roundrobinly schedule latency critical tenant
 * traffic
 */
static inline int nvme_sched_rr_subround1(void) {
    struct reflex_tenant_mgmt *thread_tenant_manager_old;
    struct nvme_sw_queue *swq;
    struct nvme_ctx *ctx;
    uint64_t t0, t1, t2, t3, t4;
    unsigned long now;
    unsigned long time_delta;
    long POS_LIMIT = 0;
    unsigned long local_leftover = 0;
    unsigned long local_demand = 0;
    double token_increment;
    int i = -1;

    now = rdtsc() / (rte_get_timer_hz() / 1e6);  // in us
    time_delta = now - percpu_get(last_sched_us);
    percpu_get(last_sched_us) = now;

    thread_tenant_manager_old = &percpu_get(reflex_tenant_manager);
    // t0 = rdtsc();

    list_for_each(&thread_tenant_manager_old->tenant_swq_head, swq, link) {
#ifndef CYCLIC_LIST
        i++;
#endif
        // serve latency-critical (LC) tenants

        if (g_nvme_fgs[swq->fg_handle].latency_critical_flag) {
#ifndef CYCLIC_LIST
            if (i < percpu_get(lc_roundrobin_start)) continue;
#endif
            token_increment =
                (g_nvme_fgs[swq->fg_handle].scaled_IOPuS_limit * time_delta) +
                0.5;  // 0.5 is for rounding
            swq->token_credit += (long)token_increment;
            swq->history_credit += (long)token_increment;
            // t2 = rdtsc();
            while ((nvme_sw_queue_isempty(swq) == 0)) {
                if (swq->token_credit <
                    -g_nvme_fgs[swq->fg_handle].deficit_limit) {
#ifdef SCHED_DEBUG
                    printf(
                        "Burst controlled: swq->token_credit %ld, "
                        "deficit_limit %ld\n",
                        swq->token_credit,
                        -g_nvme_fgs[swq->fg_handle].deficit_limit);
#endif
                    break;
                }
                if (g_outstanding_requests >= MAX_IF_REQUESTS) {
#ifndef CYCLIC_LIST
                    percpu_get(lc_roundrobin_start) = i;
#else
                    list_head_reset(&thread_tenant_manager_old->tenant_swq_head,
                                    &swq->link);
#endif
                    break;
                }
                nvme_sw_queue_pop_front(swq, &ctx);
                // t4 = rdtsc();
                issue_nvme_req(ctx);
                // possible_expensive_areas[4] += (rdtsc() - t4);
                swq->token_credit -= ctx->req_cost;
            }
            // t3 = rdtsc();
            // possible_expensive_areas[3] += (t3 - t2);

            POS_LIMIT = POS_LIMIT_FACTOR * token_increment;
            if (swq->token_credit > POS_LIMIT) {
                // #ifdef SCHED_DEBUG
                //                 printf("Before donation: swq->token_credit
                //                 %ld\n",
                //                        swq->token_credit);
                // #endif
                local_leftover += (swq->token_credit * TOKEN_FRAC_GIVEAWAY);
                swq->token_credit -= swq->token_credit * TOKEN_FRAC_GIVEAWAY;
                // #ifdef SCHED_DEBUG
                //                 printf("After donation: swq->token_credit
                //                 %ld\n",
                //                        swq->token_credit);
                // #endif
            }
        } else {  // track demand of best-effort (will need for subround2)
            local_demand += swq->total_token_demand - swq->saved_tokens;
        }
    }
#ifndef CYCLIC_LIST
    i = -1;
    list_for_each(&thread_tenant_manager->tenant_swq_head, swq, link) {
        i++;
        // serve latency-critical (LC) tenants
        if (g_nvme_fgs[swq->fg_handle].latency_critical_flag) {
            if (i >= percpu_get(lc_roundrobin_start)) break;
            token_increment =
                (g_nvme_fgs[swq->fg_handle].scaled_IOPuS_limit * time_delta) +
                0.5;  // 0.5 is for rounding
            swq->token_credit += (long)token_increment;
            swq->history_credit += (long)token_increment;
            while ((nvme_sw_queue_isempty(swq) == 0) &&
                   swq->token_credit >
                       -g_nvme_fgs[swq->fg_handle].deficit_limit) {
                if (g_outstanding_requests >= MAX_IF_REQUESTS) {
                    percpu_get(lc_roundrobin_start) = i;
                    break;
                }
                nvme_sw_queue_pop_front(swq, &ctx);
                issue_nvme_req(ctx);
                swq->token_credit -= ctx->req_cost;
            }

            POS_LIMIT = POS_LIMIT_FACTOR * token_increment;
            if (swq->token_credit > POS_LIMIT) {
                local_leftover += (swq->token_credit * TOKEN_FRAC_GIVEAWAY);
                swq->token_credit -= swq->token_credit * TOKEN_FRAC_GIVEAWAY;
            }
        } else {  // track demand of best-effort (will need for subround2)
            local_demand += swq->total_token_demand - swq->saved_tokens;
        }
    }
#endif
    // t1 = rdtsc();
    // possible_expensive_areas[2] += (t1 - t0);
    percpu_get(local_extra_demand) = local_demand;
    percpu_get(local_leftover_tokens) = local_leftover;

    return 0;
}

/*
 * nvme_sched_subround2: schedule best-effort tenant traffic
 */
static inline void nvme_sched_subround2(void) {
    struct reflex_tenant_mgmt *thread_tenant_manager_old;
    struct nvme_sw_queue *swq;
    struct nvme_ctx *ctx;
    int i;
    unsigned long local_leftover = 0;
    unsigned long local_demand = 0;
    unsigned long be_tokens = 0;
    unsigned long be_token_rate = 0;
    double token_increment = 0;
    unsigned long token_demand = 0;
    unsigned long global_tokens_acquired = 0;
    unsigned long now;
    unsigned long time_delta;

    local_leftover = percpu_get(local_leftover_tokens);
    local_demand = percpu_get(local_extra_demand);
    be_token_rate = atomic_read(&global_be_token_rate_per_tenant);

    thread_tenant_manager_old = &percpu_get(reflex_tenant_manager);

    // compare local leftover with local demand
    // synchronize access to global token bucket
    if (local_leftover > 0 &&
        local_demand == 0) {  // give away leftoever tokens to global pool
        atomic_u64_fetch_and_add(&global_leftover_tokens, local_leftover);
        return;
    } else if (local_leftover <
               local_demand) {  // try to get how much you need from global pool
        token_demand = local_demand - local_leftover;
        global_tokens_acquired =
            try_acquire_global_tokens(token_demand);  // atomic
        be_tokens = local_leftover + global_tokens_acquired;
    } else if (local_leftover >= local_demand) {
        be_tokens = local_leftover;
    }

    now = rdtsc() / (rte_get_timer_hz() / 1e6);  // in us
    time_delta = now - percpu_get(last_sched_us_be);
    percpu_get(last_sched_us_be) = now;

    // serve best effort tenants in round-robin order
    // TODO: simplify by implementing separate per-thread lists of BE and LC
    // tenants
    i = 0;
    list_for_each(&thread_tenant_manager_old->tenant_swq_head, swq, link) {
        if (i < percpu_get(roundrobin_start)) {
            i++;
            continue;
        }
        if (!g_nvme_fgs[swq->fg_handle].latency_critical_flag) {
            be_tokens += nvme_sw_queue_take_saved_tokens(swq);
            token_increment = be_token_rate / (double)1E6 * time_delta + 0.5;
            be_tokens += (long)(token_increment + 0.5);
            swq->history_credit += (long)token_increment;

            while ((nvme_sw_queue_isempty(swq) == 0) &&
                   nvme_sw_queue_peak_head_cost(swq) <= be_tokens) {
                if (g_outstanding_requests >= MAX_IF_REQUESTS) {
                    // printf("Temporal bursts! outstanding = %ld\n",
                    // g_outstanding_requests);
                    break;
                }
                nvme_sw_queue_pop_front(swq, &ctx);
                issue_nvme_req(ctx);
                be_tokens -= ctx->req_cost;
            }
            // save extra tokens for this tenant if still has demand
            be_tokens -= nvme_sw_queue_save_tokens(swq, be_tokens);
            assert(be_tokens >= 0);
        }
        i++;
    }

    int j = 0;
    list_for_each(&thread_tenant_manager_old->tenant_swq_head, swq, link) {
        if (j >= percpu_get(roundrobin_start)) {
            break;
        }
        log_debug("schedule tenant second %d\n", j);
        // log_debug("subround2: sched tenant handle %ld, tenant_tokens %lu\n",
        // swq->fg_handle, tenant_tokens);
        if (!g_nvme_fgs[swq->fg_handle].latency_critical_flag) {
            be_tokens += nvme_sw_queue_take_saved_tokens(swq);
            token_increment = be_token_rate / (double)1E6 * time_delta + 0.5;
            be_tokens += (long)(token_increment + 0.5);
            swq->history_credit += (long)token_increment;

            while ((nvme_sw_queue_isempty(swq) == 0) &&
                   nvme_sw_queue_peak_head_cost(swq) <= be_tokens) {
                if (g_outstanding_requests >= MAX_IF_REQUESTS) {
                    // printf("Temporal bursts! outstanding = %ld\n",
                    // g_outstanding_requests);
                    break;
                }
                nvme_sw_queue_pop_front(swq, &ctx);
                issue_nvme_req(ctx);
                be_tokens -= ctx->req_cost;
            }
            // save extra tokens for this tenant if still has demand
            be_tokens -= nvme_sw_queue_save_tokens(swq, be_tokens);
            assert(be_tokens >= 0);
        }
        j++;
    }

    int done = 0;
    if (thread_tenant_manager_old->num_best_effort_tenants > 0) {
        while (1) {  // find next round-robin start and check it's a best-effort
                     // tenant (otherwise unfair)
            percpu_get(roundrobin_start) =
                (percpu_get(roundrobin_start) + 1) %
                thread_tenant_manager_old->num_tenants;
            i = 0;
            list_for_each(&thread_tenant_manager_old->tenant_swq_head, swq,
                          link) {
                if (i != percpu_get(roundrobin_start)) {
                    i++;
                    continue;
                }
                if (!g_nvme_fgs[swq->fg_handle].latency_critical_flag) {
                    done = 1;  // incremented to next best effort tenant
                }
                break;
            }
            if (done == 1) {
                break;
            }
        }
    }

    if (be_tokens > 0) {
        atomic_u64_fetch_and_add(&global_leftover_tokens, be_tokens);
    }
}
#endif  // REFLEX_SCHED

/*
 * nvme_sched_en4sv0_subround1: roundrobinly schedule latency critical tenant
 * traffic
 */
static inline int nvme_sched_en4sv0_subround1(void) {
    struct en4s_tenant_mgmt *thread_tenant_manager;
    struct nvme_ctx *ctx;
    long fg_handle;
    // unsigned long now;
    unsigned long time_delta_cycles;
    // unsigned long time_delta_us;
    long POS_LIMIT = 0;
    unsigned long local_leftover = 0;
    double token_increment;
    unsigned long new_token_level = 0;
    unsigned long avail_tokens = 0;
    int i;
    uint32_t count = 0;
    int req_count = 0;
    bool work_conserving = false;

    thread_tenant_manager = &percpu_get(tenant_manager);

    iterate_active_tenants_by_type(thread_tenant_manager, lc) {
        fg_handle =
            thread_tenant_manager->active_lc_tenants[i % MAX_NVME_FLOW_GROUPS];

        // this provides better fairness for long active list!
        time_delta_cycles = rdtsc() - g_nvme_fgs[fg_handle].last_sched_time;
        g_nvme_fgs[fg_handle].last_sched_time = rdtsc();
        // printf("iterating active %ld-th tenant %ld\n", i, fg_handle);
        token_increment = g_nvme_fgs[fg_handle].scaled_IOPS_limit *
                              ((double)time_delta_cycles / rte_get_timer_hz()) +
                          0.5;
#ifdef ENABLE_KSTATS
        d_accumulated_tokens += (uint64_t)token_increment;
        d_accumulated_delta += (double)time_delta_cycles / cycles_per_us;
#endif
        g_nvme_sw_table->token_credit[fg_handle] += (int64_t)token_increment;
        while (nvme_sw_table_isempty(g_nvme_sw_table, fg_handle) == 0) {
            if (g_outstanding_requests >= MAX_IF_REQUESTS) goto V0R1_END;
            if (g_nvme_sw_table->token_credit[fg_handle] <
                -g_nvme_fgs[fg_handle].deficit_limit) {
                nvme_lc_tenant_requeue(thread_tenant_manager, fg_handle);
                count++;
                break;
            }
            nvme_sw_table_pop_front(g_nvme_sw_table, fg_handle, &ctx);
            issue_nvme_req(ctx);
            req_count++;
            // how to pay off the loan when in the work conserving mode?
            g_nvme_sw_table->token_credit[fg_handle] -= ctx->req_cost;
            // otherwise, we should have borrowed tokens from global pool
#ifdef ENABLE_KSTATS
            d_accumulated_cost += ctx->req_cost;
#endif
        }
        // this algorithm always drains the queues in the front
        if (nvme_sw_table_isempty(g_nvme_sw_table, fg_handle)) {
            // printf("active LC tenant %d now is going to be inactive\n",
            //        fg_handle);
            // printf("i=%d, head=%d, tail=%d\n", i,
            //        thread_tenant_manager->lc_head,
            //        thread_tenant_manager->lc_tail);
            count++;
        }
        if (thread_tenant_manager->num_be_tenants > 0) {
            POS_LIMIT = POS_LIMIT_FACTOR * token_increment;
            if (g_nvme_sw_table->token_credit[fg_handle] > POS_LIMIT) {
                local_leftover += (g_nvme_sw_table->token_credit[fg_handle] *
                                   TOKEN_FRAC_GIVEAWAY);
                // printf("Too many token left, donation=%ld\n",
                // local_leftover);
                d_donation_count++;
                g_nvme_sw_table->token_credit[fg_handle] -=
                    g_nvme_sw_table->token_credit[fg_handle] *
                    TOKEN_FRAC_GIVEAWAY;
            }
        }
    }
V0R1_END:
    // printf("%d LC tenants are going to be deactivated\n", count);
    nvme_lc_tenant_deactivate(thread_tenant_manager, count);
    // if (count > 0) {
    //     printf("LC tenant %ld deactivated\n", fg_handle);
    //     printf(
    //         "Tenant manager now has %d active LC tenants\n",
    //         thread_tenant_manager->lc_tail -
    //             thread_tenant_manager->lc_head);  // no mod, just for
    //             debugging
    // }
    percpu_get(local_leftover_tokens) = local_leftover;

    return req_count;
}

/*
 * nvme_sched_en4sv1_subround1: consume tokens up to demands
 * traffic
 */
static inline int nvme_sched_en4sv1_subround1(void) {
    struct en4s_tenant_mgmt *thread_tenant_manager;
    struct nvme_ctx *ctx;
    long fg_handle;
    // unsigned long now;
    unsigned long time_delta_cycles;
    // unsigned long time_delta_us;
    long POS_LIMIT = 0;
    unsigned long local_leftover = 0;
    double tot_token_increment, token_increment;
    unsigned long new_token_level = 0;
    unsigned long avail_tokens = 0;
    unsigned long lc_tokens = 0;
    int i;
    uint32_t count = 0;
    int req_count = 0;
    unsigned long now = rdtsc();
    unsigned long be_token_rate = atomic_read(&global_token_rate_per_tenant);

    thread_tenant_manager = &percpu_get(tenant_manager);

    iterate_active_tenants_by_type(thread_tenant_manager, lc) {
        fg_handle =
            thread_tenant_manager->active_lc_tenants[i % MAX_NVME_FLOW_GROUPS];

        // this provides better fairness for long active list!
        time_delta_cycles = rdtsc() - g_nvme_fgs[fg_handle].last_sched_time;
        g_nvme_fgs[fg_handle].last_sched_time = rdtsc();
        // printf("iterating active %ld-th tenant %ld\n", i, fg_handle);
        token_increment = g_nvme_fgs[fg_handle].scaled_IOPS_limit *
                              ((double)time_delta_cycles / rte_get_timer_hz()) +
                          0.5;
        lc_tokens = nvme_sw_table_take_saved_tokens(g_nvme_sw_table, fg_handle);

        lc_tokens += (int64_t)token_increment;
        while (nvme_sw_table_isempty(g_nvme_sw_table, fg_handle) == 0) {
            if (g_outstanding_requests >= MAX_IF_REQUESTS) goto V0R1_END;
            if (nvme_sw_table_peak_head_cost(g_nvme_sw_table, fg_handle) >
                lc_tokens) {
                nvme_lc_tenant_requeue(thread_tenant_manager, fg_handle);
                count++;
                break;
            }
            nvme_sw_table_pop_front(g_nvme_sw_table, fg_handle, &ctx);
            issue_nvme_req(ctx);
            req_count++;
            // how to pay off the loan when in the work conserving mode?
            lc_tokens -= ctx->req_cost;
            // otherwise, we should have borrowed tokens from global pool
        }

        // this algorithm always drains the queues in the front
        if (nvme_sw_table_isempty(g_nvme_sw_table, fg_handle)) {
            count++;
        }
        lc_tokens -=
            nvme_sw_table_save_tokens(g_nvme_sw_table, fg_handle, lc_tokens);
        if (lc_tokens > 0) {
            local_leftover += lc_tokens;
        }
    }
V0R1_END:
    nvme_lc_tenant_deactivate(thread_tenant_manager, count);
    percpu_get(local_leftover_tokens) = local_leftover;

    return req_count;
}

/*
 * nvme_schedv1_subround2: consume remaining tokens fairly
 */
static inline int nvme_sched_en4sv1_subround2(bool smoothy) {
    struct en4s_tenant_mgmt *thread_tenant_manager;
    struct sw_counter *thread_rw_counter;
    struct nvme_ctx *ctx;
    long fg_handle;
    int i;
    int req_count = 0;
    unsigned long local_leftover = 0;
    unsigned long local_demand = 0;
    unsigned long be_tokens = 0;
    unsigned long be_token_rate = 0;
    double token_increment = 0;
    unsigned long token_demand = 0;
    unsigned long global_tokens_acquired = 0;
    unsigned long now;
    unsigned long time_delta_cycles;
    uint32_t count = 0;

    local_leftover = percpu_get(local_leftover_tokens);
    be_token_rate = atomic_read(&global_token_rate_per_tenant);

    thread_tenant_manager = &percpu_get(tenant_manager);
    thread_rw_counter = &percpu_get(rw_counter);

    local_demand =
        g_nvme_sw_table->be_token_demand - g_nvme_sw_table->be_saved_tokens;
    // compare local leftover with local demand
    // synchronize access to global token bucket
    if (local_leftover > 0 &&
        local_demand == 0) {  // give away leftoever tokens to global pool
        atomic_u64_fetch_and_add(&global_leftover_tokens, local_leftover);
    } else if (local_leftover < local_demand) {  // try to get how much you
                                                 // need from global pool
        token_demand = local_demand - local_leftover;
        global_tokens_acquired =
            try_acquire_global_tokens(token_demand);  // atomic
        be_tokens = local_leftover + global_tokens_acquired;
    } else if (local_leftover >= local_demand) {
        be_tokens = local_leftover;
    }

    time_delta_cycles = now - percpu_get(last_sched_cycles_be);

    iterate_active_tenants_by_type(thread_tenant_manager, lc) {
        fg_handle =
            thread_tenant_manager->active_lc_tenants[i % MAX_NVME_FLOW_GROUPS];
        token_increment =
            be_token_rate * ((double)time_delta_cycles / rte_get_timer_hz()) +
            0.5;
        be_tokens = nvme_sw_table_take_saved_tokens(g_nvme_sw_table, fg_handle);

        be_tokens += (int64_t)token_increment;
        while (nvme_sw_table_isempty(g_nvme_sw_table, fg_handle) == 0) {
            if (g_outstanding_requests >= MAX_IF_REQUESTS) goto V1R2_END;
            if (nvme_sw_table_peak_head_cost(g_nvme_sw_table, fg_handle) >
                be_tokens) {
                nvme_lc_tenant_requeue(thread_tenant_manager, fg_handle);
                count++;
                break;
            }
            nvme_sw_table_pop_front(g_nvme_sw_table, fg_handle, &ctx);
            issue_nvme_req(ctx);
            req_count++;
            // how to pay off the loan when in the work conserving mode?
            be_tokens -= ctx->req_cost;
            // otherwise, we should have borrowed tokens from global pool
        }

        // this algorithm always drains the queues in the front
        if (nvme_sw_table_isempty(g_nvme_sw_table, fg_handle)) {
            count++;
        }
        be_tokens -=
            nvme_sw_table_save_tokens(g_nvme_sw_table, fg_handle, be_tokens);
    }

    // serve best effort tenants in round-robin order
    iterate_active_tenants_by_type(thread_tenant_manager, be) {
        fg_handle =
            thread_tenant_manager->active_be_tenants[i % MAX_NVME_FLOW_GROUPS];
        be_tokens +=
            nvme_sw_table_take_saved_tokens(g_nvme_sw_table, fg_handle);
        time_delta_cycles = rdtsc() - g_nvme_fgs[fg_handle].last_sched_time;
        g_nvme_fgs[fg_handle].last_sched_time = rdtsc();
        token_increment =
            be_token_rate * ((double)time_delta_cycles / rte_get_timer_hz());
        be_tokens += (long)(token_increment + 0.5);

        while (nvme_sw_table_isempty(g_nvme_sw_table, fg_handle) == 0) {
            if (g_outstanding_requests >= MAX_IF_REQUESTS) {
                // at least one element & the demand > 0
                be_tokens -= nvme_sw_table_save_tokens(g_nvme_sw_table,
                                                       fg_handle, be_tokens);
                goto V1R2_END;
            }
            if (nvme_sw_table_peak_head_cost(g_nvme_sw_table, fg_handle) >
                be_tokens) {
                nvme_be_tenant_requeue(thread_tenant_manager, fg_handle);
                count++;
                break;
            }
            nvme_sw_table_pop_front(g_nvme_sw_table, fg_handle, &ctx);
            issue_nvme_req(ctx);
            req_count++;
            be_tokens -= ctx->req_cost;
            // smoothy schedule allows at most one at a time
            if (smoothy &&
                nvme_sw_table_isempty(g_nvme_sw_table, fg_handle) == 0) {
                nvme_be_tenant_requeue(thread_tenant_manager, fg_handle);
                count++;
                break;
            }
        }

        if (nvme_sw_table_isempty(g_nvme_sw_table, fg_handle)) {
            count++;
        }
        be_tokens -=
            nvme_sw_table_save_tokens(g_nvme_sw_table, fg_handle, be_tokens);
    }

V1R2_END:
    nvme_be_tenant_deactivate(thread_tenant_manager, count);

    if (be_tokens > 0) {
        atomic_u64_fetch_and_add(&global_leftover_tokens, be_tokens);
    }

#ifdef FAST_JOIN
// dequeue requests from slow joining tenants with fcfs
#endif
    return req_count;
}

/*
 * nvme_sched_en4sv1: initial smooth schedule implementation, provides fairer
 * performance for tenants of same demands
 */
static inline int nvme_sched_en4sv01_subround1(void) {
    struct en4s_tenant_mgmt *thread_tenant_manager;
    struct nvme_ctx *ctx;
    long fg_handle;
    unsigned long time_delta_cycles;
    long POS_LIMIT = 0;
    unsigned long local_leftover = 0;
    unsigned long local_demand = 0;
    double token_increment;
    int i;
    int req_count = 0;
    uint32_t count = 0;
    uint32_t dyn_smooth_share = 0;
    uint16_t issued = 0;

    thread_tenant_manager = &percpu_get(tenant_manager);

    iterate_active_tenants_by_type(thread_tenant_manager, lc) {
        fg_handle =
            thread_tenant_manager->active_lc_tenants[i % MAX_NVME_FLOW_GROUPS];
        issued = 0;

        // this provides better fairness for long active list!
        time_delta_cycles = rdtsc() - g_nvme_fgs[fg_handle].last_sched_time;
        g_nvme_fgs[fg_handle].last_sched_time = rdtsc();
        // printf("iterating active %ld-th tenant %ld\n", i, fg_handle);
        token_increment = g_nvme_fgs[fg_handle].scaled_IOPS_limit *
                              ((double)time_delta_cycles / rte_get_timer_hz()) +
                          0.5;
#ifdef ENABLE_KSTATS
        d_accumulated_tokens += (uint64_t)token_increment;
        d_accumulated_delta += (double)time_delta_cycles / cycles_per_us;
#endif
        g_nvme_sw_table->token_credit[fg_handle] += (int64_t)token_increment;
        dyn_smooth_share =
            (uint32_t)(nvme_sw_table_count(g_nvme_sw_table, fg_handle) / 2.0 +
                       0.5);
        while (nvme_sw_table_isempty(g_nvme_sw_table, fg_handle) == 0) {
            if (g_outstanding_requests >= MAX_IF_REQUESTS) goto V1R1_END;
            if (g_nvme_sw_table->token_credit[fg_handle] <
                    -g_nvme_fgs[fg_handle].deficit_limit
                // || issued >= g_nvme_fgs[fg_handle].smooth_share
                || issued >= dyn_smooth_share) {
                // requeue happens more often cos limit is lower
                nvme_lc_tenant_requeue(thread_tenant_manager, fg_handle);
                count++;
                break;
            }
            nvme_sw_table_pop_front(g_nvme_sw_table, fg_handle, &ctx);
            issue_nvme_req(ctx);
            req_count++;
            g_nvme_sw_table->token_credit[fg_handle] -= ctx->req_cost;
            issued++;
        }

        // it becomes empty and deactivated
        if (nvme_sw_table_isempty(g_nvme_sw_table, fg_handle)) {
            count++;
        }
        if (!g_nvme_sw_table->critical_count &&
            thread_tenant_manager->num_be_tenants > 0) {
            POS_LIMIT = POS_LIMIT_FACTOR * token_increment;
            if (g_nvme_sw_table->token_credit[fg_handle] > POS_LIMIT) {
                local_leftover += (g_nvme_sw_table->token_credit[fg_handle] *
                                   TOKEN_FRAC_GIVEAWAY);
                d_donation_count++;
                g_nvme_sw_table->token_credit[fg_handle] -=
                    g_nvme_sw_table->token_credit[fg_handle] *
                    TOKEN_FRAC_GIVEAWAY;
            }
        }
    }
V1R1_END:
    nvme_lc_tenant_deactivate(thread_tenant_manager, count);

    percpu_get(local_leftover_tokens) = local_leftover;

    return req_count;
}

/*
 * nvme_sched_en4sv2_subround1: pool the burst allowance for LC tenants
 * traffic
 */
static inline int nvme_sched_en4sv2_subround1(void) {
    struct en4s_tenant_mgmt *thread_tenant_manager;
    struct nvme_ctx *ctx;
    long fg_handle;
    unsigned long time_delta_cycles;
    long POS_LIMIT = 0;
    unsigned long local_demand = 0;
    double token_increment, tot_token_increment;
    unsigned long new_token_level = 0;
    unsigned long avail_tokens = 0;
    unsigned long giveaway_tokens = 0;
    int i;
    int req_count = 0;
    uint32_t count = 0;
    thread_tenant_manager = &percpu_get(tenant_manager);

    time_delta_cycles = rdtsc() - percpu_get(last_sched_cycles_lc);
    percpu_get(last_sched_cycles_lc) = rdtsc();
    tot_token_increment = global_LC_sum_token_rate *
                              ((double)time_delta_cycles / rte_get_timer_hz()) +
                          0.5;
    g_nvme_sw_table->total_token_credit += (int64_t)tot_token_increment;

    iterate_active_tenants_by_type(thread_tenant_manager, lc) {
        fg_handle =
            thread_tenant_manager->active_lc_tenants[i % MAX_NVME_FLOW_GROUPS];
        time_delta_cycles = rdtsc() - g_nvme_fgs[fg_handle].last_sched_time;
        g_nvme_fgs[fg_handle].last_sched_time = rdtsc();
        token_increment = g_nvme_fgs[fg_handle].scaled_IOPS_limit *
                              ((double)time_delta_cycles / rte_get_timer_hz()) +
                          0.5;
        g_nvme_sw_table->token_credit[fg_handle] += (int64_t)token_increment;

#ifdef ENABLE_KSTATS
        d_accumulated_tokens += (uint64_t)token_increment;
        d_accumulated_delta += (double)time_delta_cycles / cycles_per_us;
#endif

        if (nvme_sw_table_isempty(g_nvme_sw_table, fg_handle) == 0 &&
            g_nvme_sw_table->token_credit[fg_handle] <
                -(g_nvme_fgs[fg_handle].deficit_limit /
                  TOKEN_DEFICIT_LIMIT_DIV)) {
            g_nvme_sw_table->loan_times[fg_handle] += 1;
            if (g_nvme_sw_table->loan_times[fg_handle] > max_loan_times) {
                printf(
                    "Tenant %ld (IOPS:%d) just created a new loam time records "
                    "at %d\n",
                    fg_handle, g_nvme_fgs[fg_handle].IOPS_SLO,
                    g_nvme_sw_table->loan_times[fg_handle]);
                max_loan_times = g_nvme_sw_table->loan_times[fg_handle];
            }
            if (g_nvme_sw_table->loan_times[fg_handle] >
                TOKEN_DEFICIT_MAX_TIMES) {
                g_nvme_sw_table->loan_times[fg_handle] -=
                    TOKEN_DEFICIT_MAX_TIMES;
                g_nvme_fgs[fg_handle].deficit_limit =
                    g_nvme_fgs[fg_handle].deficit_limit / 2;
                // printf("Tenant %ld (IOPS:%d) deficit limit halved to %ld\n",
                //        fg_handle, g_nvme_fgs[fg_handle].IOPS_SLO,
                //        g_nvme_fgs[fg_handle].deficit_limit);
            }
        } else {
            // loan paid off!
            g_nvme_sw_table->loan_times[fg_handle] = 0;
            g_nvme_fgs[fg_handle].deficit_limit =
                g_nvme_fgs[fg_handle].scaled_IOPS_limit *
                TOKEN_DEFICIT_LIMIT_PERC / 100;
            ;
            // printf("Tenant %ld (IOPS:%d) deficit limit reset\n", fg_handle,
            //        g_nvme_fgs[fg_handle].IOPS_SLO);
        }
        while (nvme_sw_table_isempty(g_nvme_sw_table, fg_handle) == 0) {
            if (g_outstanding_requests >= MAX_IF_REQUESTS) goto V2R1_END;
            if (g_nvme_sw_table->token_credit[fg_handle] <
                    -g_nvme_fgs[fg_handle].deficit_limit
                // || g_nvme_sw_table->loan_times[fg_handle] > TOKEN_DEFICIT_MAX_TIMES
                ) {
                nvme_lc_tenant_requeue(thread_tenant_manager, fg_handle);
                count++;
                break;
            }
            nvme_sw_table_pop_front(g_nvme_sw_table, fg_handle, &ctx);
            issue_nvme_req(ctx);
            req_count++;
            // how to pay off the loan when in the work conserving mode?
            g_nvme_sw_table->token_credit[fg_handle] -= ctx->req_cost;
            g_nvme_sw_table->total_token_credit -= ctx->req_cost;
            // otherwise, we should have borrowed tokens from global pool
#ifdef ENABLE_KSTATS
            d_accumulated_cost += ctx->req_cost;
#endif
        }
        // this algorithm always drains the queues in the front
        if (nvme_sw_table_isempty(g_nvme_sw_table, fg_handle)) {
            count++;
        }
        if (thread_tenant_manager->num_be_tenants > 0) {
            POS_LIMIT = POS_LIMIT_FACTOR * token_increment;
            if (g_nvme_sw_table->token_credit[fg_handle] > POS_LIMIT) {
                giveaway_tokens = (g_nvme_sw_table->token_credit[fg_handle] *
                                   TOKEN_FRAC_GIVEAWAY);
                percpu_get(local_leftover_tokens) += giveaway_tokens;
                // printf("Too many token left, donation=%ld\n",
                // local_leftover);
                d_donation_count++;
                g_nvme_sw_table->total_token_credit -= giveaway_tokens;
                g_nvme_sw_table->token_credit[fg_handle] -= giveaway_tokens;
            }
        }
    }
V2R1_END:
    nvme_lc_tenant_deactivate(thread_tenant_manager, count);
    // if (thread_tenant_manager->num_be_tenants > 0) {
    //     POS_LIMIT = POS_LIMIT_FACTOR * tot_token_increment;
    //     if (g_nvme_sw_table->total_token_credit > POS_LIMIT) {
    //         percpu_get(local_leftover_tokens) =
    //             (g_nvme_sw_table->total_token_credit * TOKEN_FRAC_GIVEAWAY);
    //         g_nvme_sw_table->total_token_credit -=
    //             g_nvme_sw_table->total_token_credit * TOKEN_FRAC_GIVEAWAY;
    //     }
    // }
    // percpu_get(local_leftover_tokens) = local_leftover;

    return req_count;
}

/*
 * nvme_sched_en4sv02_subround1: shortest flow first
 * traffic
 */
static inline int nvme_sched_en4sv02_subround1(void) {
    struct en4s_tenant_mgmt *thread_tenant_manager;
    struct nvme_ctx *ctx;
    long fg_handle;
    // unsigned long now;
    unsigned long time_delta_cycles;
    // unsigned long time_delta_us;
    long POS_LIMIT = 0;
    unsigned long local_leftover = 0;
    double token_increment;
    unsigned long new_token_level = 0;
    unsigned long avail_tokens = 0;
    int i;
    uint32_t count = 0;
    int req_count = 0;

    thread_tenant_manager = &percpu_get(tenant_manager);

    // hardcoded the IOPS skip for now
    for (int min_iops_slo = 100; min_iops_slo <= 1000; min_iops_slo += 100) {
        iterate_active_tenants_by_type(thread_tenant_manager, lc) {
            fg_handle = thread_tenant_manager
                            ->active_lc_tenants[i % MAX_NVME_FLOW_GROUPS];
            if (g_nvme_fgs[fg_handle].IOPS_SLO < min_iops_slo ||
                g_nvme_fgs[fg_handle].IOPS_SLO >= min_iops_slo + 100) {
                continue;
            }

            // this provides better fairness for long active list!
            time_delta_cycles = rdtsc() - g_nvme_fgs[fg_handle].last_sched_time;
            g_nvme_fgs[fg_handle].last_sched_time = rdtsc();
            // printf("iterating active %ld-th tenant %ld\n", i, fg_handle);
            token_increment =
                g_nvme_fgs[fg_handle].scaled_IOPS_limit *
                    ((double)time_delta_cycles / rte_get_timer_hz()) +
                0.5;
            g_nvme_sw_table->token_credit[fg_handle] +=
                (int64_t)token_increment;
            while (nvme_sw_table_isempty(g_nvme_sw_table, fg_handle) == 0) {
                if (g_outstanding_requests >= MAX_IF_REQUESTS) goto V2R1_END;
                if (g_nvme_sw_table->token_credit[fg_handle] <
                    -g_nvme_fgs[fg_handle].deficit_limit) {
                    nvme_lc_tenant_requeue(thread_tenant_manager, fg_handle);
                    count++;
                    break;
                }
                nvme_sw_table_pop_front(g_nvme_sw_table, fg_handle, &ctx);
                issue_nvme_req(ctx);
                req_count++;
                // how to pay off the loan when in the work conserving mode?
                g_nvme_sw_table->token_credit[fg_handle] -= ctx->req_cost;
                // otherwise, we should have borrowed tokens from global pool
            }
            // this algorithm always drains the queues in the front
            if (nvme_sw_table_isempty(g_nvme_sw_table, fg_handle)) {
                count++;
            }
            if (thread_tenant_manager->num_be_tenants > 0) {
                POS_LIMIT = POS_LIMIT_FACTOR * token_increment;
                if (g_nvme_sw_table->token_credit[fg_handle] > POS_LIMIT) {
                    local_leftover +=
                        (g_nvme_sw_table->token_credit[fg_handle] *
                         TOKEN_FRAC_GIVEAWAY);
                    d_donation_count++;
                    g_nvme_sw_table->token_credit[fg_handle] -=
                        g_nvme_sw_table->token_credit[fg_handle] *
                        TOKEN_FRAC_GIVEAWAY;
                }
            }
        }
    }
    assert(count == (thread_tenant_manager->lc_tail + MAX_NVME_FLOW_GROUPS -
                     thread_tenant_manager->lc_head) %
                        MAX_NVME_FLOW_GROUPS);
V2R1_END:
    nvme_lc_tenant_deactivate(thread_tenant_manager, count);
    percpu_get(local_leftover_tokens) = local_leftover;

    return req_count;
}

/*
 * nvme_sched_en4sv2_subround1: high read ratio first
 * traffic
 */
static inline int nvme_sched_en4sv03_subround1(void) {
    struct en4s_tenant_mgmt *thread_tenant_manager;
    struct nvme_ctx *ctx;
    long fg_handle;
    // unsigned long now;
    unsigned long time_delta_cycles;
    // unsigned long time_delta_us;
    long POS_LIMIT = 0;
    unsigned long local_leftover = 0;
    unsigned long local_demand = 0;
    double token_increment;
    unsigned long new_token_level = 0;
    unsigned long avail_tokens = 0;
    int i;
    int req_count = 0;
    uint32_t count = 0;
    bool work_conserving = false;

    thread_tenant_manager = &percpu_get(tenant_manager);

    if (g_nvme_sw_table->total_request_count + g_outstanding_requests <
        MAX_IF_REQUESTS) {
        // FIXME: this does not consider different request costs
        work_conserving = true;
    }

    // hardcoded the ratio skip for now
    for (int min_rw_ratio = 100; min_rw_ratio >= 0; min_rw_ratio -= 10) {
        iterate_active_tenants_by_type(thread_tenant_manager, lc) {
            fg_handle = thread_tenant_manager
                            ->active_lc_tenants[i % MAX_NVME_FLOW_GROUPS];
            if (g_nvme_fgs[fg_handle].rw_ratio_SLO < min_rw_ratio ||
                g_nvme_fgs[fg_handle].rw_ratio_SLO >= min_rw_ratio + 10) {
                continue;
            }

            // this provides better fairness for long active list!
            time_delta_cycles = rdtsc() - g_nvme_fgs[fg_handle].last_sched_time;
            g_nvme_fgs[fg_handle].last_sched_time = rdtsc();
            // printf("iterating active %ld-th tenant %ld\n", i, fg_handle);
            token_increment =
                g_nvme_fgs[fg_handle].scaled_IOPS_limit *
                    ((double)time_delta_cycles / rte_get_timer_hz()) +
                0.5;
#ifdef ENABLE_KSTATS
            d_accumulated_tokens += (uint64_t)token_increment;
            d_accumulated_delta += (double)time_delta_cycles / cycles_per_us;
#endif
            g_nvme_sw_table->token_credit[fg_handle] +=
                (int64_t)token_increment;
            while (nvme_sw_table_isempty(g_nvme_sw_table, fg_handle) == 0) {
                if (g_outstanding_requests >= MAX_IF_REQUESTS) goto V2R1_END;
                if (!work_conserving) {
                    if (g_nvme_sw_table->token_credit[fg_handle] <
                        -g_nvme_fgs[fg_handle].deficit_limit) {
                        nvme_lc_tenant_requeue(thread_tenant_manager,
                                               fg_handle);
                        count++;
                        break;
                    }
                }
                nvme_sw_table_pop_front(g_nvme_sw_table, fg_handle, &ctx);
                issue_nvme_req(ctx);
                req_count++;
                // how to pay off the loan when in the work conserving mode?
                g_nvme_sw_table->token_credit[fg_handle] -= ctx->req_cost;
                // otherwise, we should have borrowed tokens from global pool
#ifdef ENABLE_KSTATS
                d_accumulated_cost += ctx->req_cost;
#endif
            }
            // this algorithm always drains the queues in the front
            if (nvme_sw_table_isempty(g_nvme_sw_table, fg_handle)) {
                // printf("active LC tenant %d now is going to be inactive\n",
                //        fg_handle);
                // printf("i=%d, head=%d, tail=%d\n", i,
                //        thread_tenant_manager->lc_head,
                //        thread_tenant_manager->lc_tail);
                count++;
            }
            if (thread_tenant_manager->num_be_tenants > 0) {
                POS_LIMIT = POS_LIMIT_FACTOR * token_increment;
                if (g_nvme_sw_table->token_credit[fg_handle] > POS_LIMIT) {
                    local_leftover +=
                        (g_nvme_sw_table->token_credit[fg_handle] *
                         TOKEN_FRAC_GIVEAWAY);
                    // printf("Too many token left, donation=%ld\n",
                    // local_leftover);
                    d_donation_count++;
                    g_nvme_sw_table->token_credit[fg_handle] -=
                        g_nvme_sw_table->token_credit[fg_handle] *
                        TOKEN_FRAC_GIVEAWAY;
                }
            }
        }
    }
    assert(count == (thread_tenant_manager->lc_tail + MAX_NVME_FLOW_GROUPS -
                     thread_tenant_manager->lc_head) %
                        MAX_NVME_FLOW_GROUPS);
V2R1_END:
    // printf("%d LC tenants are going to be deactivated\n", count);
    nvme_lc_tenant_deactivate(thread_tenant_manager, count);
    // if (count > 0) {
    //     printf("LC tenant %ld deactivated\n", fg_handle);
    //     printf(
    //         "Tenant manager now has %d active LC tenants\n",
    //         thread_tenant_manager->lc_tail -
    //             thread_tenant_manager->lc_head);  // no mod, just for
    //             debugging
    // }
    percpu_get(local_leftover_tokens) = local_leftover;

    return req_count;
}

/*
 * nvme_sched_en4sv3_subround1: control the inflight write ratio
 * traffic
 */
static inline int nvme_sched_en4sv3_subround1(void) {
    struct en4s_tenant_mgmt *thread_tenant_manager;
    struct sw_counter *thread_rw_counter;
    struct nvme_ctx *ctx;
    long fg_handle;
    // unsigned long now;
    unsigned long time_delta_cycles;
    // unsigned long time_delta_us;
    long POS_LIMIT = 0;
    unsigned long local_leftover = 0;
    unsigned long local_demand = 0;
    double token_increment;
    unsigned long new_token_level = 0;
    unsigned long avail_tokens = 0;
    int i;
    int req_count = 0;
    uint32_t count = 0;

    thread_tenant_manager = &percpu_get(tenant_manager);
    thread_rw_counter = &percpu_get(rw_counter);

    iterate_active_tenants_by_type(thread_tenant_manager, lc) {
        fg_handle =
            thread_tenant_manager->active_lc_tenants[i % MAX_NVME_FLOW_GROUPS];

        // this provides better fairness for long active list!
        time_delta_cycles = rdtsc() - g_nvme_fgs[fg_handle].last_sched_time;
        g_nvme_fgs[fg_handle].last_sched_time = rdtsc();
        // printf("iterating active %ld-th tenant %ld\n", i, fg_handle);
        token_increment = g_nvme_fgs[fg_handle].scaled_IOPS_limit *
                              ((double)time_delta_cycles / rte_get_timer_hz()) +
                          0.5;
#ifdef ENABLE_KSTATS
        d_accumulated_tokens += (uint64_t)token_increment;
        d_accumulated_delta += (double)time_delta_cycles / cycles_per_us;
#endif
        g_nvme_sw_table->token_credit[fg_handle] += (int64_t)token_increment;
        while (nvme_sw_table_isempty(g_nvme_sw_table, fg_handle) == 0) {
            if (g_outstanding_requests >= MAX_IF_REQUESTS) goto V3R1_END;
            if (nvme_sw_table_peak_head_cmd(g_nvme_sw_table, fg_handle) ==
                    NVME_CMD_WRITE &&
                get_ratio(thread_rw_counter, 1) > g_wr_ratio_limit) {
                nvme_lc_tenant_requeue(thread_tenant_manager, fg_handle);
                count++;
                break;
            }
            if (g_nvme_sw_table->token_credit[fg_handle] <
                -g_nvme_fgs[fg_handle].deficit_limit) {
                nvme_lc_tenant_requeue(thread_tenant_manager, fg_handle);
                count++;
                break;
            }
            nvme_sw_table_pop_front(g_nvme_sw_table, fg_handle, &ctx);
            issue_nvme_req(ctx);
            req_count++;
            // how to pay off the loan when in the work conserving mode?
            g_nvme_sw_table->token_credit[fg_handle] -= ctx->req_cost;
            // otherwise, we should have borrowed tokens from global pool
#ifdef ENABLE_KSTATS
            d_accumulated_cost += ctx->req_cost;
#endif
        }
        // this algorithm always drains the queues in the front
        if (nvme_sw_table_isempty(g_nvme_sw_table, fg_handle)) {
            // printf("active LC tenant %d now is going to be inactive\n",
            //        fg_handle);
            // printf("i=%d, head=%d, tail=%d\n", i,
            //        thread_tenant_manager->lc_head,
            //        thread_tenant_manager->lc_tail);
            count++;
        }
        if (thread_tenant_manager->num_be_tenants > 0) {
            POS_LIMIT = POS_LIMIT_FACTOR * token_increment;
            if (g_nvme_sw_table->token_credit[fg_handle] > POS_LIMIT) {
                local_leftover += (g_nvme_sw_table->token_credit[fg_handle] *
                                   TOKEN_FRAC_GIVEAWAY);
                // printf("Too many token left, donation=%ld\n",
                // local_leftover);
                d_donation_count++;
                g_nvme_sw_table->token_credit[fg_handle] -=
                    g_nvme_sw_table->token_credit[fg_handle] *
                    TOKEN_FRAC_GIVEAWAY;
            }
        }
    }
V3R1_END:
    nvme_lc_tenant_deactivate(thread_tenant_manager, count);
    percpu_get(local_leftover_tokens) = local_leftover;

    return req_count;
}

/*
 * nvme_sched_en4sv4: control lc requests & queue occupancy
 */
static inline int nvme_sched_en4sv4_subround1(void) {
    struct en4s_tenant_mgmt *thread_tenant_manager;
    struct nvme_ctx *ctx;
    long fg_handle;
    unsigned long time_delta_cycles;
    long POS_LIMIT = 0;
    unsigned long local_leftover = 0;
    unsigned long local_demand = 0;
    double token_increment;
    unsigned long new_token_level = 0;
    unsigned long avail_tokens = 0;
    int i;
    int req_count = 0;
    uint32_t count = 0;
    bool work_conserving = false;

    thread_tenant_manager = &percpu_get(tenant_manager);

    if (g_nvme_sw_table->critical_count + g_outstanding_requests <
        MAX_IF_REQUESTS / 4) {
        // FIXME: we should borrow tokens from BE tenants
        work_conserving = true;
    }

    iterate_active_tenants_by_type(thread_tenant_manager, lc) {
        fg_handle =
            thread_tenant_manager->active_lc_tenants[i % MAX_NVME_FLOW_GROUPS];

        // this provides better fairness for long active list!
        time_delta_cycles = rdtsc() - g_nvme_fgs[fg_handle].last_sched_time;
        g_nvme_fgs[fg_handle].last_sched_time = rdtsc();
        // printf("iterating active %ld-th tenant %ld\n", i, fg_handle);
        token_increment = g_nvme_fgs[fg_handle].scaled_IOPS_limit *
                              ((double)time_delta_cycles / rte_get_timer_hz()) +
                          0.5;
#ifdef ENABLE_KSTATS
        d_accumulated_tokens += (uint64_t)token_increment;
        d_accumulated_delta += (double)time_delta_cycles / cycles_per_us;
#endif
        g_nvme_sw_table->token_credit[fg_handle] += (int64_t)token_increment;
        while (nvme_sw_table_isempty(g_nvme_sw_table, fg_handle) == 0) {
            if (g_outstanding_requests >= MAX_IF_REQUESTS) break;
            if (!work_conserving) {
                if (g_nvme_sw_table->token_credit[fg_handle] <
                    -g_nvme_fgs[fg_handle].deficit_limit) {
                    nvme_lc_tenant_requeue(thread_tenant_manager, fg_handle);
                    count++;
                    break;
                }
            }
            nvme_sw_table_pop_front(g_nvme_sw_table, fg_handle, &ctx);
            issue_nvme_req(ctx);
            req_count++;
            // how to pay off the loan when in the work conserving mode?
            g_nvme_sw_table->token_credit[fg_handle] -= ctx->req_cost;
            // otherwise, we should have borrowed tokens from global pool
#ifdef ENABLE_KSTATS
            d_accumulated_cost += ctx->req_cost;
#endif
        }
        // this algorithm always drains the queues in the front
        if (nvme_sw_table_isempty(g_nvme_sw_table, fg_handle)) {
            // printf("active LC tenant %d now is going to be inactive\n",
            //        fg_handle);
            // printf("i=%d, head=%d, tail=%d\n", i,
            //        thread_tenant_manager->lc_head,
            //        thread_tenant_manager->lc_tail);
            count++;
        }
        if (thread_tenant_manager->num_be_tenants > 0) {
            POS_LIMIT = POS_LIMIT_FACTOR * token_increment;
            if (g_nvme_sw_table->token_credit[fg_handle] > POS_LIMIT) {
                local_leftover += (g_nvme_sw_table->token_credit[fg_handle] *
                                   TOKEN_FRAC_GIVEAWAY);
                // printf("Too many token left, donation=%ld\n",
                // local_leftover);
                d_donation_count++;
                g_nvme_sw_table->token_credit[fg_handle] -=
                    g_nvme_sw_table->token_credit[fg_handle] *
                    TOKEN_FRAC_GIVEAWAY;
            }
        }
    }
    // printf("%d LC tenants are going to be deactivated\n", count);
    nvme_lc_tenant_deactivate(thread_tenant_manager, count);
    // if (count > 0) {
    //     printf("LC tenant %ld deactivated\n", fg_handle);
    //     printf(
    //         "Tenant manager now has %d active LC tenants\n",
    //         thread_tenant_manager->lc_tail -
    //             thread_tenant_manager->lc_head);  // no mod, just for
    //             debugging
    // }
    percpu_get(local_leftover_tokens) = local_leftover;

    return req_count;
}

/*
 * nvme_sched_subround2: schedule best-effort tenant traffic
 */
static inline int nvme_sched_en4s_subround2(bool smoothy) {
    struct en4s_tenant_mgmt *thread_tenant_manager;
    struct sw_counter *thread_rw_counter;
    struct nvme_ctx *ctx;
    long fg_handle;
    int i;
    int req_count = 0;
    unsigned long local_leftover = 0;
    unsigned long local_demand = 0;
    unsigned long be_tokens = 0;
    unsigned long be_token_rate = 0;
    double token_increment = 0;
    unsigned long token_demand = 0;
    unsigned long global_tokens_acquired = 0;
    unsigned long now;
    unsigned long time_delta_cycles;
    uint32_t count = 0;

    local_leftover = percpu_get(local_leftover_tokens);
    be_token_rate = atomic_read(&global_be_token_rate_per_tenant);

    thread_tenant_manager = &percpu_get(tenant_manager);
    thread_rw_counter = &percpu_get(rw_counter);

    local_demand =
        g_nvme_sw_table->be_token_demand - g_nvme_sw_table->be_saved_tokens;
    // compare local leftover with local demand
    // synchronize access to global token bucket
    if (local_leftover > 0 &&
        local_demand == 0) {  // give away leftoever tokens to global pool
        atomic_u64_fetch_and_add(&global_leftover_tokens, local_leftover);
    } else if (local_leftover < local_demand) {  // try to get how much you
                                                 // need from global pool
        token_demand = local_demand - local_leftover;
        global_tokens_acquired =
            try_acquire_global_tokens(token_demand);  // atomic
        be_tokens = local_leftover + global_tokens_acquired;
    } else if (local_leftover >= local_demand) {
        be_tokens = local_leftover;
    }

    // serve best effort tenants in round-robin order
    iterate_active_tenants_by_type(thread_tenant_manager, be) {
        fg_handle =
            thread_tenant_manager->active_be_tenants[i % MAX_NVME_FLOW_GROUPS];
        be_tokens +=
            nvme_sw_table_take_saved_tokens(g_nvme_sw_table, fg_handle);
        time_delta_cycles = rdtsc() - g_nvme_fgs[fg_handle].last_sched_time;
        g_nvme_fgs[fg_handle].last_sched_time = rdtsc();
        token_increment =
            be_token_rate * ((double)time_delta_cycles / rte_get_timer_hz());
        be_tokens += (long)(token_increment + 0.5);

        while (nvme_sw_table_isempty(g_nvme_sw_table, fg_handle) == 0) {
            if (g_outstanding_requests >= MAX_IF_REQUESTS) {
                // at least one element & the demand > 0
                be_tokens -= nvme_sw_table_save_tokens(g_nvme_sw_table,
                                                       fg_handle, be_tokens);
                goto V0R2_END;
            }
            if (nvme_sw_table_peak_head_cost(g_nvme_sw_table, fg_handle) >
                be_tokens) {
                nvme_be_tenant_requeue(thread_tenant_manager, fg_handle);
                count++;
                break;
            }
            nvme_sw_table_pop_front(g_nvme_sw_table, fg_handle, &ctx);
            issue_nvme_req(ctx);
            req_count++;
            be_tokens -= ctx->req_cost;
            // smoothy schedule allows at most one at a time
            if (smoothy &&
                nvme_sw_table_isempty(g_nvme_sw_table, fg_handle) == 0) {
                nvme_be_tenant_requeue(thread_tenant_manager, fg_handle);
                count++;
                break;
            }
        }

        if (nvme_sw_table_isempty(g_nvme_sw_table, fg_handle)) {
            count++;
        }
        // if not smoothy then the queue can be empty & the demand is 0
        // or the first request cost is too high, demand > tokens, then
        // be_tokens = 0

        // WARNNING this will become a live lock with
        // closed-loop/feedback-controlled senders because the table only take
        // up to the demand, but if the demand is not consumed, it won't
        // increase the demand; if no demand, it won't save tokens, so the
        // tokens will be wasted
        be_tokens -=
            nvme_sw_table_save_tokens(g_nvme_sw_table, fg_handle, be_tokens);
    }

V0R2_END:
    nvme_be_tenant_deactivate(thread_tenant_manager, count);

    if (be_tokens > 0) {
        atomic_u64_fetch_and_add(&global_leftover_tokens, be_tokens);
    }

#ifdef FAST_JOIN
// dequeue requests from slow joining tenants with fcfs
#endif
    return req_count;
}

/*
 * nvme_sched_en4sv01_subround2: RR for all BE requests
 */
static inline int nvme_sched_en4sv11_subround2(bool smoothy) {
    struct en4s_tenant_mgmt *thread_tenant_manager;
    struct nvme_ctx *ctx;
    long fg_handle;
    unsigned long be_tokens = 0;
    unsigned long be_tokens_ind = 0;
    unsigned long be_token_rate = 0;
    double token_increment = 0;
    unsigned long be_tokens_last = 0;
    unsigned long now;
    unsigned long time_delta_cycles;
    uint32_t count = 0;
    int demand;
    int i;
    int req_count = 0;

    thread_tenant_manager = &percpu_get(tenant_manager);
    // from round 1
    be_tokens = percpu_get(local_leftover_tokens);
#ifndef SINGLE_THREADED
    // compare demand and acquire tokens
    log_err("multi-threaded round 2 not implemented");
#else
    // only collect last round of tokens
    be_tokens_last = atomic64_read(&global_leftover_tokens);
    be_tokens += be_tokens_last;
#endif
    // this rate can be flexible
    be_token_rate = atomic_read(&global_be_token_rate_per_tenant);

    // serve best effort tenants in round-robin order
    iterate_active_tenants_by_type(thread_tenant_manager, be) {
        fg_handle =
            thread_tenant_manager->active_be_tenants[i % MAX_NVME_FLOW_GROUPS];
        time_delta_cycles = rdtsc() - g_nvme_fgs[fg_handle].last_sched_time;
        g_nvme_fgs[fg_handle].last_sched_time = rdtsc();
        // be_tokens_ind =
        //     nvme_sw_table_take_saved_tokens(g_nvme_sw_table, fg_handle);

        token_increment =
            be_token_rate * ((double)time_delta_cycles / rte_get_timer_hz());
        // unused tokens will be given to the next tenant
        // be_tokens_ind += (long)token_increment;
        g_nvme_sw_table->token_credit[fg_handle] += (int64_t)token_increment;

        while (nvme_sw_table_isempty(g_nvme_sw_table, fg_handle) == 0) {
            if (g_outstanding_requests >= MAX_IF_REQUESTS) goto V2R2_END;

            demand = nvme_sw_table_peak_head_cost(g_nvme_sw_table, fg_handle);
            if (demand > g_nvme_sw_table->token_credit[fg_handle] &&
                demand > be_tokens) {
                nvme_be_tenant_requeue(thread_tenant_manager, fg_handle);
                count++;
                break;
            }
            nvme_sw_table_pop_front(g_nvme_sw_table, fg_handle, &ctx);
            issue_nvme_req(ctx);
            req_count++;
            if (ctx->req_cost < g_nvme_sw_table->token_credit[fg_handle]) {
                g_nvme_sw_table->token_credit[fg_handle] -= ctx->req_cost;
            } else {
                be_tokens -=
                    (ctx->req_cost - g_nvme_sw_table->token_credit[fg_handle]);
                g_nvme_sw_table->token_credit[fg_handle] = 0;
                break;
            }
            // smoothy schedule allows at most one at a time
            if (smoothy &&
                nvme_sw_table_isempty(g_nvme_sw_table, fg_handle) == 0) {
                nvme_be_tenant_requeue(thread_tenant_manager, fg_handle);
                count++;
                break;
            }
        }

        // if (be_tokens_ind > 0) {
        //     ret = nvme_sw_table_save_tokens(g_nvme_sw_table, fg_handle,
        //                               be_tokens_ind);
        // }
        if (nvme_sw_table_isempty(g_nvme_sw_table, fg_handle)) {
            count++;
        }
    }

V2R2_END:
    if (count == nvme_be_active_tenant_count(thread_tenant_manager)) {
        // some tenants are deactivated once, and will be requeued
        nvme_be_tenant_deactivate(thread_tenant_manager, count);
        // after deactivation, some tenants are still active
        if (!nvme_be_tenant_isempty(thread_tenant_manager)) {
            // move the front tenant to the back to round-robin
            nvme_be_tenant_requeue(thread_tenant_manager,
                                   thread_tenant_manager->be_head);
            nvme_be_tenant_deactivate(thread_tenant_manager, 1);
        }  // else all tenants are deactivated and emptied
    } else {  // else some tenants are still active but blocked because of
              // queue's full
        assert(count < nvme_be_active_tenant_count(thread_tenant_manager));
        nvme_be_tenant_deactivate(thread_tenant_manager, count);
    }

    be_tokens -= be_tokens_last;
    // give away leftover tokens of this round to next round
    if (be_tokens > 0) {
        atomic64_write(&global_leftover_tokens, be_tokens);
    } else {
        // used the leftover tokens from last round
        atomic64_write(&global_leftover_tokens, 0);
    }

#ifdef FAST_JOIN
// dequeue requests from slow joining tenants with fcfs
#endif
    return req_count;
}

/*
 * nvme_sched_en4sv13_subround2: FCFS for all BE requests
 */
static inline int nvme_sched_en4sv13_subround2(bool smoothy) {
    struct en4s_tenant_mgmt *thread_tenant_manager;
    struct nvme_ctx *ctx;
    long fg_handle;
    unsigned long be_tokens = 0;
    unsigned long be_token_rate = 0;
    double token_increment = 0;
    unsigned long be_tokens_last = 0;
    unsigned long now;
    unsigned long time_delta_cycles;
    uint32_t count = 0;
    int i;
    int req_count = 0;

    thread_tenant_manager = &percpu_get(tenant_manager);
    // from round 1
    be_tokens = percpu_get(local_leftover_tokens);
#ifndef SINGLE_THREADED
    // compare demand and acquire tokens
    log_err("multi-threaded round 2 not implemented");
#else
    // only collect last round of tokens
    be_tokens_last = atomic64_read(&global_leftover_tokens);
    be_tokens += be_tokens_last;
#endif
    // this rate can be flexible
    be_token_rate = atomic_read(&global_be_token_rate_per_tenant);

    // serve best effort tenants in round-robin order
    iterate_active_tenants_by_type(thread_tenant_manager, be) {
        fg_handle =
            thread_tenant_manager->active_be_tenants[i % MAX_NVME_FLOW_GROUPS];
        time_delta_cycles = rdtsc() - g_nvme_fgs[fg_handle].last_sched_time;
        g_nvme_fgs[fg_handle].last_sched_time = rdtsc();
        token_increment =
            be_token_rate * ((double)time_delta_cycles / rte_get_timer_hz());
        // unused tokens will be given to the next tenant
        be_tokens += (long)token_increment;

        while (nvme_sw_table_isempty(g_nvme_sw_table, fg_handle) == 0) {
            if (g_outstanding_requests >= MAX_IF_REQUESTS) goto V1R2_END;
            if (nvme_sw_table_peak_head_cost(g_nvme_sw_table, fg_handle) >
                be_tokens) {
                nvme_be_tenant_requeue(thread_tenant_manager, fg_handle);
                count++;
                break;
            }
            nvme_sw_table_pop_front(g_nvme_sw_table, fg_handle, &ctx);
            issue_nvme_req(ctx);
            req_count++;
            be_tokens -= ctx->req_cost;
            // smoothy schedule allows at most one at a time
            if (smoothy &&
                nvme_sw_table_isempty(g_nvme_sw_table, fg_handle) == 0) {
                nvme_be_tenant_requeue(thread_tenant_manager, fg_handle);
                count++;
                break;
            }
        }

        if (nvme_sw_table_isempty(g_nvme_sw_table, fg_handle)) {
            count++;
        }
    }

V1R2_END:
    nvme_be_tenant_deactivate(thread_tenant_manager, count);

    be_tokens -= be_tokens_last;
    // give away leftover tokens of this round to next round
    if (be_tokens > 0) {
        atomic64_write(&global_leftover_tokens, be_tokens);
    } else {
        // used the leftover tokens from last round
        atomic64_write(&global_leftover_tokens, 0);
    }

#ifdef FAST_JOIN
// dequeue requests from slow joining tenants with fcfs
#endif
}

/*
 * nvme_sched_en4sv11_subround2: another FCFS for all BE requests
 */
static inline int nvme_sched_en4sv12_subround2(bool smoothy) {
    struct nvme_ctx *ctx;
    long fg_handle;
    uint32_t count = 0;
    int req_count = 0;
    unsigned long now = rdtsc();
    struct en4s_tenant_mgmt *thread_tenant_manager =
        &percpu_get(tenant_manager);
    unsigned long time_delta_cycles = now - percpu_get(last_sched_cycles_be);
    double token_increment = atomic_read(&global_be_token_rate_per_tenant) *
                             thread_tenant_manager->num_be_tenants *
                             ((double)time_delta_cycles / rte_get_timer_hz());
    unsigned long be_tokens = atomic_u64_read(&global_leftover_tokens) +
                              (long)(token_increment + 0.5);

    percpu_get(last_sched_cycles_be) = now;
    iterate_active_tenants_by_type(thread_tenant_manager, be) {
        fg_handle =
            thread_tenant_manager->active_be_tenants[i % MAX_NVME_FLOW_GROUPS];

        // cond 1
        while (nvme_sw_table_isempty(g_nvme_sw_table, fg_handle) == 0) {
            // cond 2 & 3, not deactivating tenant
            if (g_outstanding_requests >= MAX_IF_REQUESTS ||
                nvme_sw_table_peak_head_cost(g_nvme_sw_table, fg_handle) >
                    be_tokens)
                goto V11R2END;
            nvme_sw_table_pop_front(g_nvme_sw_table, fg_handle, &ctx);
            issue_nvme_req(ctx);
            be_tokens -= ctx->req_cost;
            // cond 4
            if (smoothy &&
                nvme_sw_table_isempty(g_nvme_sw_table, fg_handle) == 0) {
                nvme_be_tenant_requeue(thread_tenant_manager, fg_handle);
                count++;
                break;
            }
        }

        // cond 1
        if (nvme_sw_table_isempty(g_nvme_sw_table, fg_handle)) {
            count++;
        }
    }
V11R2END:
    nvme_be_tenant_deactivate(thread_tenant_manager, count);

    if (be_tokens > 0) {
        atomic_u64_write(&global_leftover_tokens, be_tokens);
    }
    return req_count;
}

/*
 * nvme_sched_en4sv2_subround2: RR deactivation
 */
static inline int nvme_sched_en4sv2_subround2(bool smoothy) {
    struct en4s_tenant_mgmt *thread_tenant_manager;
    struct sw_counter *thread_rw_counter;
    struct nvme_ctx *ctx;
    long fg_handle;
    int i;
    int req_count = 0;
    unsigned long local_leftover = 0;
    unsigned long local_demand = 0;
    unsigned long be_tokens = 0;
    unsigned long be_token_rate = 0;
    double token_increment = 0;
    unsigned long token_demand = 0;
    unsigned long global_tokens_acquired = 0;
    unsigned long now;
    unsigned long time_delta_cycles;
    uint32_t count = 0;

    local_leftover = percpu_get(local_leftover_tokens);
    be_token_rate = atomic_read(&global_be_token_rate_per_tenant);

    thread_tenant_manager = &percpu_get(tenant_manager);
    thread_rw_counter = &percpu_get(rw_counter);

    local_demand =
        g_nvme_sw_table->be_token_demand - g_nvme_sw_table->be_saved_tokens;
    // compare local leftover with local demand
    // synchronize access to global token bucket
    if (local_leftover > 0 &&
        local_demand == 0) {  // give away leftoever tokens to global pool
        atomic_u64_fetch_and_add(&global_leftover_tokens, local_leftover);
    } else if (local_leftover < local_demand) {  // try to get how much you
                                                 // need from global pool
        token_demand = local_demand - local_leftover;
        global_tokens_acquired =
            try_acquire_global_tokens(token_demand);  // atomic
        be_tokens = local_leftover + global_tokens_acquired;
    } else if (local_leftover >= local_demand) {
        be_tokens = local_leftover;
    }

    // serve best effort tenants in round-robin order
    iterate_active_tenants_by_type(thread_tenant_manager, be) {
        fg_handle =
            thread_tenant_manager->active_be_tenants[i % MAX_NVME_FLOW_GROUPS];
        be_tokens +=
            nvme_sw_table_take_saved_tokens(g_nvme_sw_table, fg_handle);
        time_delta_cycles = rdtsc() - g_nvme_fgs[fg_handle].last_sched_time;
        g_nvme_fgs[fg_handle].last_sched_time = rdtsc();
        token_increment =
            be_token_rate * ((double)time_delta_cycles / rte_get_timer_hz());
        be_tokens += (long)(token_increment + 0.5);

        while (nvme_sw_table_isempty(g_nvme_sw_table, fg_handle) == 0) {
            if (g_outstanding_requests >= MAX_IF_REQUESTS) {
                // at least one element & the demand > 0
                be_tokens -= nvme_sw_table_save_tokens(g_nvme_sw_table,
                                                       fg_handle, be_tokens);
                goto V2R2_END;
            }
            if (nvme_sw_table_peak_head_cost(g_nvme_sw_table, fg_handle) >
                be_tokens) {
                nvme_be_tenant_requeue(thread_tenant_manager, fg_handle);
                count++;
                break;
            }
            nvme_sw_table_pop_front(g_nvme_sw_table, fg_handle, &ctx);
            issue_nvme_req(ctx);
            req_count++;
            be_tokens -= ctx->req_cost;
            // smoothy schedule allows at most one at a time
            if (smoothy &&
                nvme_sw_table_isempty(g_nvme_sw_table, fg_handle) == 0) {
                nvme_be_tenant_requeue(thread_tenant_manager, fg_handle);
                count++;
                break;
            }
        }

        if (nvme_sw_table_isempty(g_nvme_sw_table, fg_handle)) {
            count++;
        }
        // if not smoothy
        // then the queue can be empty & the demand is 0
        // or the first request cost is too high, demand > tokens, then
        // be_tokens = 0
        be_tokens -=
            nvme_sw_table_save_tokens(g_nvme_sw_table, fg_handle, be_tokens);
    }

V2R2_END:
    if (count == nvme_be_active_tenant_count(thread_tenant_manager)) {
        // some tenants are deactivated once, and will be requeued
        nvme_be_tenant_deactivate(thread_tenant_manager, count);
        // after deactivation, some tenants are still active
        if (!nvme_be_tenant_isempty(thread_tenant_manager)) {
            // move the front tenant to the back to round-robin
            nvme_be_tenant_requeue(thread_tenant_manager,
                                   thread_tenant_manager->be_head);
            nvme_be_tenant_deactivate(thread_tenant_manager, 1);
        }  // else all tenants are deactivated and emptied
    } else {  // else some tenants are still active but blocked because of
              // queue's full
        assert(count < nvme_be_active_tenant_count(thread_tenant_manager));
        nvme_be_tenant_deactivate(thread_tenant_manager, count);
    }

    if (be_tokens > 0) {
        atomic_u64_fetch_and_add(&global_leftover_tokens, be_tokens);
    }

#ifdef FAST_JOIN
// dequeue requests from slow joining tenants with fcfs
#endif
    return req_count;
}

int nvme_sched(void) {
    // KSTATS_PUSH(nvme_sched, NULL);
    // if (percpu_get(g_sched_time) > rdtsc()) return 0;
    // percpu_get(g_sched_time) = rdtsc() + g_sched_interval;
#ifdef NO_SCHED
    // KSTATS_POP(NULL);
    return 0;
#endif
    int r1_ret, r2_ret;
    struct nvme_ctx *pctx;
    struct en4s_tenant_mgmt *thread_tenant_manager =
        &percpu_get(tenant_manager);

#ifdef REFLEX_SCHED
    struct reflex_tenant_mgmt *thread_tenant_manager_old =
        &percpu_get(reflex_tenant_manager);
    if (thread_tenant_manager_old->num_tenants ==
        thread_tenant_manager_old->num_best_effort_tenants) {
        percpu_get(last_sched_us) = rdtsc() / rte_get_timer_hz() * 1e6;
    }
    if (thread_tenant_manager_old->num_best_effort_tenants == 0) {
        percpu_get(last_sched_us_be) = rdtsc() / rte_get_timer_hz() * 1e6;
    }
#endif

    if (!g_nvme_sched_mode) {
#ifdef REFLEX_SCHED
        while (nvme_sw_queue_isempty(&percpu_get(g_default_swq)) == 0) {
            if (g_outstanding_requests >= MAX_IF_REQUESTS) break;
            nvme_sw_queue_pop_front(&percpu_get(g_default_swq), &pctx);
            issue_nvme_req(pctx);
        }
#else
        while (nvme_sw_table_isempty(g_nvme_sw_table, 0) == 0) {
            if (g_outstanding_requests >= MAX_IF_REQUESTS) break;

            nvme_sw_table_pop_front(g_nvme_sw_table, 0, &pctx);
            issue_nvme_req(pctx);
        }
#endif

        return 0;
    }

    if (percpu_get(tenant_manager).num_lc_tenants == 0) {
        percpu_get(last_sched_cycles_lc) = rdtsc();
    }
    if (percpu_get(tenant_manager).num_be_tenants == 0) {
        percpu_get(last_sched_cycles_be) = rdtsc();
        update_scheduled_bitvector();
    }

    switch (g_nvme_sched_mode) {
#ifdef REFLEX_SCHED
        case REFLEX:
            if (thread_tenant_manager_old->num_tenants == 0) {
                update_scheduled_bitvector();
                return 0;
            }
            r1_ret = nvme_sched_subround1();
            if (!r1_ret) nvme_sched_subround2();
            update_scheduled_bitvector();
            break;
        case REFLEX_RR:
            if (thread_tenant_manager_old->num_tenants == 0) {
                update_scheduled_bitvector();
                return 0;
            }
            // roundrobinly serve lc tenants
            r1_ret = nvme_sched_rr_subround1();
            if (!r1_ret) nvme_sched_subround2();
            update_scheduled_bitvector();
            break;
#endif
        case En4Sv0:
            if (thread_tenant_manager->num_lc_tenants == 0 &&
                thread_tenant_manager->num_be_tenants == 0) {
                return;
            }
            r1_ret = nvme_sched_en4sv0_subround1();
            if (r1_ret >= 0) r2_ret = nvme_sched_en4s_subround2(false);
            update_scheduled_bitvector();
            break;
        case En4Sv01:
            if (thread_tenant_manager->num_lc_tenants == 0 &&
                thread_tenant_manager->num_be_tenants == 0) {
                return;
            }
            r1_ret = nvme_sched_en4sv0_subround1();
            if (r1_ret >= 0) r2_ret = nvme_sched_en4s_subround2(false);
            if (percpu_get(g_sched_time) <= rdtsc()) {
                percpu_get(g_sched_time) = rdtsc() + g_sched_interval;
                update_scheduled_bitvector();
            }
            break;
        case En4Sv02:
            if (thread_tenant_manager->num_lc_tenants == 0 &&
                thread_tenant_manager->num_be_tenants == 0) {
                return;
            }
            r1_ret = nvme_sched_en4sv0_subround1();
            if (r1_ret >= 0) r2_ret = nvme_sched_en4sv2_subround2(false);
            if (percpu_get(g_sched_time) <= rdtsc()) {
                percpu_get(g_sched_time) = rdtsc() + g_sched_interval;
                update_scheduled_bitvector();
            }
            break;
        case En4Sv10:
            if (thread_tenant_manager->num_lc_tenants == 0 &&
                thread_tenant_manager->num_be_tenants == 0)
                return;
            r1_ret = nvme_sched_en4sv1_subround1();
            if (r1_ret >= 0) r2_ret = nvme_sched_en4s_subround2(false);
            update_scheduled_bitvector();
            break;
        case En4Sv11:
            r1_ret = nvme_sched_en4sv1_subround1();
            if (r1_ret >= 0) r2_ret = nvme_sched_en4sv1_subround2(false);
            break;
        case En4Sv12:
            r1_ret = nvme_sched_en4sv1_subround1();
            if (r1_ret >= 0) r2_ret = nvme_sched_en4sv2_subround2(false);
            break;
        case En4Sv20:
            if (thread_tenant_manager->num_lc_tenants == 0 &&
                thread_tenant_manager->num_be_tenants == 0)
                return;
            r1_ret = nvme_sched_en4sv2_subround1();
            if (r1_ret >= 0) r2_ret = nvme_sched_en4s_subround2(false);
            update_scheduled_bitvector();
            break;
        case En4Sv21:
            if (thread_tenant_manager->num_lc_tenants == 0 &&
                thread_tenant_manager->num_be_tenants == 0)
                return;
            r1_ret = nvme_sched_en4sv2_subround1();
            if (r1_ret >= 0) r2_ret = nvme_sched_en4s_subround2(false);
            if (percpu_get(g_sched_time) <= rdtsc()) {
                percpu_get(g_sched_time) = rdtsc() + g_sched_interval;
                update_scheduled_bitvector();
            }
            break;
        case En4Sv22:
            if (thread_tenant_manager->num_lc_tenants == 0 &&
                thread_tenant_manager->num_be_tenants == 0)
                return;
            r1_ret = nvme_sched_en4sv2_subround1();
            if (r1_ret >= 0) r2_ret = nvme_sched_en4sv2_subround2(false);
            if (percpu_get(g_sched_time) <= rdtsc()) {
                percpu_get(g_sched_time) = rdtsc() + g_sched_interval;
                update_scheduled_bitvector();
            }
            break;
        case En4Sv30:
            if (thread_tenant_manager->num_lc_tenants == 0 &&
                thread_tenant_manager->num_be_tenants == 0)
                return;
            r1_ret = nvme_sched_en4sv3_subround1();
            if (r1_ret >= 0) r2_ret = nvme_sched_en4s_subround2(false);
            update_scheduled_bitvector();
            break;
        default:
            // handle other cases or error
            log_err("Unknown scheduling mode %d\n", g_nvme_sched_mode);
            break;
    }
    // if (g_outstanding_requests)
    //     printf("%d %d %ld %ld\n", r1_ret, r2_ret, g_outstanding_requests,
    //            g_outstanding_lc_requests);
    percpu_get(local_leftover_tokens) = 0;
#ifdef REFLEX_SCHED
    percpu_get(local_extra_demand) = 0;
#endif
    // KSTATS_POP(NULL);

    return 0;
}

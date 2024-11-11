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
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
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
 * control_plane.h - control plane definitions
 */

#include <ix/compiler.h>
#include <ix/ethfg.h>
#include <rte_per_lcore.h>

#define IDLE_FIFO_SIZE 256

struct cpu_metrics {
    double queuing_delay;
    double batch_size;
    double queue_size[3];
    long loop_duration;
    double idle[3];
} __aligned(64);

struct flow_group_metrics {
    int cpu;
} __aligned(64);

enum cpu_state {
    CP_CPU_STATE_IDLE = 0,
    CP_CPU_STATE_RUNNING,
};

enum commands {
    CP_CMD_NOP = 0,
    CP_CMD_MIGRATE,
    CP_CMD_IDLE,
};

enum status {
    CP_STATUS_READY = 0,
    CP_STATUS_RUNNING,
};

struct command_struct {
    enum cpu_state cpu_state;
    enum commands cmd_id;
    enum status status;
    union {
        struct {
            DEFINE_BITMAP(fg_bitmap, ETH_MAX_TOTAL_FG);
            int cpu;
        } migrate;
        struct {
            char fifo[IDLE_FIFO_SIZE];
        } idle;
    };
    char no_idle;
};

extern volatile struct cp_shmem {
    uint32_t nr_flow_groups;
    uint32_t nr_cpus;
    float pkg_power;
    int cpu[NCPU];
    struct cpu_metrics cpu_metrics[NCPU];
    struct flow_group_metrics flow_group[ETH_MAX_TOTAL_FG];
    struct command_struct command[NCPU];
    uint32_t cycles_per_us;
    uint32_t scratchpad_idx;
    struct {
        long remote_queue_pkts_begin;
        long remote_queue_pkts_end;
        long local_queue_pkts;
        long backlog_before;
        long backlog_after;
        long timers;
        long timer_fired;
        long ts_migration_start;
        long ts_data_structures_done;
        long ts_before_backlog;
        long ts_after_backlog;
        long ts_migration_end;
        long ts_first_pkt_at_prev;
        long ts_last_pkt_at_prev;
        long ts_first_pkt_at_target;
        long ts_last_pkt_at_target;
    } scratchpad[1024];
} * cp_shmem;

#define SCRATCHPAD (&cp_shmem->scratchpad[cp_shmem->scratchpad_idx])
#define SCRATCHPAD_NEXT                            \
    do {                                           \
        assert(++cp_shmem->scratchpad_idx < 1024); \
    } while (0)

RTE_DECLARE_PER_LCORE(volatile struct command_struct *, cp_cmd);
RTE_DECLARE_PER_LCORE(unsigned long, idle_cycles);

void cp_idle(void);

static inline double ema_update(double prv_value, double value, double alpha) {
    return alpha * value + (1 - alpha) * prv_value;
}

#define EMA_UPDATE(ema, value, alpha) ema = ema_update(ema, value, alpha)

extern double energy_unit;

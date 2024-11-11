#include "sys_arch.h"

#include <ix/log.h>
#include <lwip/opt.h>
#include <lwip/sys.h>
#include <rte_cycles.h>
#include <rte_per_lcore.h>

int sys_time_init() {
    cycles_per_ms = rte_get_timer_hz() / 1000;
    cycles_per_us = rte_get_timer_hz() / 1000000;
    log_info("cycles_per_ms: %u, cycles_per_us: %lu\n", cycles_per_ms,
             cycles_per_us);
    return 0;
}

/* Time in Milliseconds */
u32_t sys_now(void) { return rdtsc() / cycles_per_ms; }

/* Time in Microseconds */
u64_t sys_now_us(void) { return rdtsc() / cycles_per_us; }

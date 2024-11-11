#pragma once

#include <lwip/def.h>

u32_t cycles_per_ms;
u64_t cycles_per_us;

int sys_time_init();
u32_t sys_now(void);
u64_t sys_now_us(void);
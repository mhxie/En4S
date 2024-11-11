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
 * chksum.h - utilities for calculating checksums
 */

#pragma once

#include <ix/types.h>

/**
 * chksum_internet - performs an internet checksum on a buffer
 * @buf: the buffer
 * @len: the length in bytes
 *
 * An internet checksum is a 16-bit one's complement sum. Details
 * are described in RFC 1071.
 *
 * Returns a 16-bit checksum value.
 */
static inline uint16_t chksum_internet(const char *buf, int len) {
    uint64_t sum = 0;  // 64bit
// printf("Calculating chksum, buf is %s, len is %d\n", buf, len);
#if defined(__i386__) || defined(__x86_64__)
    asm volatile(
        "xorq %0, %0\n"

        /* process 8 byte chunks */
        "movl %2, %%edx\n"
        "shrl $3, %%edx\n"
        "cmp $0, %%edx\n"
        "jz 2f\n"
        "1: adcq (%1), %0\n"
        "leaq 8(%1), %1\n"
        "decl %%edx\n"
        "jne 1b\n"
        "adcq $0, %0\n"

        /* process 4 byte (if left) */
        "2: test $4, %2\n"
        "je 3f\n"
        "movl (%1), %%edx\n"
        "addq %%rdx, %0\n"
        "adcq $0, %0\n"
        "leaq 4(%1), %1\n"

        /* process 2 byte (if left) */
        "3: test $2, %2\n"
        "je 4f\n"
        "movzxw (%1), %%rdx\n"
        "addq %%rdx, %0\n"
        "adcq $0, %0\n"
        "leaq 2(%1), %1\n"

        /* process 1 byte (if left) */
        "4: test $1, %2\n"
        "je 5f\n"
        "movzxb (%1), %%rdx\n"
        "addq %%rdx, %0\n"
        "adcq $0, %0\n"

        /* fold into 16-bit answer */
        "5: movq %0, %1\n"
        "shrq $32, %0\n"
        "addl %k1, %k0\n"
        "adcl $0, %k0\n"
        "movq %0, %1\n"
        "shrl $16, %k0\n"
        "addw %w1, %w0\n"
        "adcw $0, %w0\n"
        "not %0\n"

        : "=&r"(sum), "=r"(buf)
        : "r"(len), "1"(buf)
        : "%rdx", "cc", "memory");
// #elif defined(__aarch64__)
// 	asm volatile(

// 		: [sum] "=&r"(sum), [buf]"=r"(buf)
// 		: [len] "r"(len), [buf]"1"(buf)
// 		: "cc", "memory"
// 	);
#else
        // while (len >= 8) {
    // 	sum += *((uint64_t *) buf);
    // 	buf += 8;
    // 	len -= 8;
    // }
    // while (len >= 4) {
    // 	sum += *((uint32_t *) buf);
    // 	buf += 4;
    // 	len -= 4;
    // }
    while (len >= 2) {
        sum += *((uint16_t *)buf);
        buf += 2;
        len -= 2;
    }

    if (len == 1) {
        sum = sum + *((uint8_t *)buf);
    }

    while (sum >> 16)
        sum = (sum & 0xFFFF) + (sum >> 16);

    sum = ~sum;
#endif
    return (uint16_t)sum;
}

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

enum msg_type {
    PUT,
    GET,
    PUT_ACK,
    GET_RESP,
};

struct msg_header {
    void *addr;
    int cmd;
    size_t len;
    int tag;
};

/*
 * ReFlex protocol support
 */

#define CMD_GET 0x00
#define CMD_SET 0x01
#define CMD_SET_NO_ACK 0x02
#define CMD_REG 0x03

#define RESP_OK 0x00
#define RESP_EINVAL 0x04

#define REQ_PKT 0x80
#define RESP_PKT 0x81
#define MAX_EXTRA_LEN 8
#define MAX_KEY_LEN 8

typedef struct __attribute__((__packed__)) {
    uint32_t IOPS_SLO;
    uint8_t rw_ratio_SLO;
    uint8_t latency_SLO_hi;
    uint16_t latency_SLO_lo;
} slo_t;

typedef struct __attribute__((__packed__)) {
    uint32_t supply;
    uint32_t assigned;
} token_t;

typedef struct __attribute__((__packed__)) {
    uint16_t magic;
    uint16_t opcode;
    union {
        void *req_handle;
        void *flow_handle;
    };
    union {
        // CMD_GET or CMD_SET
        uint64_t lba;           // request
        uint64_t service_time;  // response
        // CMD_REG
        slo_t SLO;        // request
        int64_t SLO_val;  // request
        token_t token;    // response
    };
    union {
        // CMD_GET or CMD_SET
        uint32_t lba_count;  // request
        // CMD_REG
        int32_t token_demand;  // request
        // CMD_GET or CMD_SET or CMD_REG
        uint32_t resp_code;  // response
    };
} binary_header_blk_t;

void *pp_main(void *arg);

# cython: language_level=3
# reflex.pxd
#
# ReFlex Cython Header

cimport cpython


# cdef extern from "reflex.h":
ctypedef packed struct slo_t:
    unsigned int IOPS_SLO
    unsigned char rw_ratio
    unsigned char latency_SLO_hi
    unsigned short latency_SLO_lo

ctypedef packed struct token_t:
    unsigned int supply
    unsigned int assigned

# FIXME: fix this when Cython supports anonymous unions
cdef union SLO_union:
    unsigned long lba
    unsigned long service_time
    slo_t SLO
    token_t token
    # unsigned long SLO_val # server-side id

cdef union flow_union:
    unsigned int lba_count
    unsigned int token_demand
    unsigned int resp_code

ctypedef packed struct binary_header_blk_t:
    short magic
    short opcode
    unsigned long req_handle
    SLO_union u1
    flow_union u2

# ctypedef union reflex_header:
#     binary_header_blk_t fields
#     char raw_bytes[24]

cdef enum:
    CMD_GET,
    CMD_SET,
    CMD_SET_NO_ACK,
    CMD_REG

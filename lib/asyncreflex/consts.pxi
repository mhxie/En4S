# cython: language_level=3
# consts.pxi
#
# Commonly used constants

# controller op code

DEF CTRL_REG = b'\x00'
DEF CTRL_DEREG = b'\x01'
DEF CTRL_UPDATE = b'\x02'
DEF CTRL_ALLOC = b'\x03'
DEF CTRL_DEALLOC = b'\x04'

# controller/storage response code
DEF RESP_OK = b'\x00'
DEF RESP_FAIL = b'\x01'
DEF RESP_EINVAL = b'\x04'

DEF LBA_SIZE = 512            # bytes per logical block
DEF SEND_MAX = 256             # number of in-flight lbas, to support 1MB object, 2048 is needed
DEF SEND_HDR_DEPTH = 64       # depth of send header queue
DEF ENABLE_BLOAT = False      # enable/disable request bloat
DEF LAMBDA_BW = 81920         # lambda bandwidth in KiB/s
DEF LAMBDA_CL_IOPS = 1000     # lambda client closed-loop IOPS
DEF ASYNC_OVERHEAD = 10       # async overhead in us
DEF ADAPTIVE_LEVEL = 0        # adaptive level
DEF CALIBRATE_INTERVAL = 1e8  # calibrate interval in ns

DEF ENABLE_DEBUG = 0          # debug switch
DEF ENABLE_STAT = 0           # statistic switch
DEF ENABLE_FC = 1             # flow control switch
DEF CORE_IMPL = 0             # implementation choice: 0: protocol, 1: stream
DEF TRACE_BUF = 0             # buffer size trace switch
DEF SELECTOR_TIMEOUT = 0      # selector timeout, in seconds
DEF CONN_CLOSE_TIMEOUT = 5 # connection close timeout, in seconds
DEF MAX_TO_SEND = 4096
DEF MAX_TO_RECV = 16480
DEF MAX_RECV_PENDING = 131840  # 32*(4096+24)
DEF EWMA_ALPHA = 0.8
DEF EWMA_BETA = 0.2

# cython: language_level=3
# cython: profile=False
# socket.pyx
#
# Socket-based implementation

cimport cython
# cimport cpython

from cpython.buffer cimport PyObject_GetBuffer, PyBuffer_Release
from cpython.buffer cimport PyBUF_ANY_CONTIGUOUS, PyBUF_SIMPLE
from cpython.ref cimport PyObject
from libc.stdlib cimport malloc, free

include "reflex.pxd"
include "consts.pxi"

import socket
import collections
import selectors
import time

from asyncreflex.flow import FlowEnforcer
from asyncreflex.stats import ConnStats

import logging
logger = logging.getLogger(__name__)

cdef short header_nbytes = sizeof(binary_header_blk_t)
cdef Py_buffer buffer


def first(iterable):
    """ get the first element of an iterable """
    return next(iter(iterable))


cdef class ReFlexSocket:
    cdef public:
        dict pending_requests, completed_requests
    cdef object _sock, _flow_enforcer, _sockopt
    cdef object _recv_buffer, _app_buffer, _send_buffer, _deferred_buffer
    cdef object _blens, _stats
    cdef object _selector
    cdef list pending_request_handles

    cdef unsigned int IOPS_SLO, latency_us_SLO, _pending_bytes
    cdef unsigned _inflights, _inflight_lbas, _max_inflights, _max_inflights_lbas
    cdef unsigned char rw_ratio
    cdef bint _recv_pending, _registered, _qc_enabled, _fc_enabled, _sca_enabled, _writable
    cdef unsigned int sent_header_pos
    cdef double fixed_interval, _next_req_time, _close_start_t
    cdef binary_header_blk_t *recv_header
    cdef binary_header_blk_t *reg_header
    cdef binary_header_blk_t *sent_headers[SEND_HDR_DEPTH]

    def __init__(self, opts):
        # header allocation
        self.recv_header = <binary_header_blk_t *> malloc(header_nbytes)
        self.reg_header = <binary_header_blk_t *> malloc(header_nbytes)
        for i in range(SEND_HDR_DEPTH):
            self.sent_headers[i] = <binary_header_blk_t *> malloc(header_nbytes)

        if "sockopts" in opts.keys():
            self._sockopt = opts["sockopts"]
        else:
            self._sockopt = []
        if "nonblocking" in opts.keys():
            self._selector = selectors.DefaultSelector()
        else:
            self._selector = None
        self.init_new()

        # init only once
        self._qc_enabled = False
        self._fc_enabled = False
        if "send_max" in opts.keys():
            self._max_inflights = opts["send_max"]
            self._max_inflights_lbas = opts["send_max"] * 8
        else:
            self._max_inflights = SEND_MAX
            self._max_inflights_lbas = SEND_MAX * 8
        if "enable_fc" in opts.keys():
            self._flow_enforcer = FlowEnforcer()
            if opts["enable_fc"] >= 1:
                self._qc_enabled = True
                # statically-sized window
            if opts["enable_fc"] >= 2:
                self._fc_enabled = True
                # dynamically resized windows
            if opts["enable_fc"] >= 3:
                # storage-congestion-aware resized windows
                self._sca_enabled = True
        self.pending_requests = {}
        self.completed_requests = {}


    cpdef init_new(self):
        # socket-specific initialization
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._recv_buffer = bytearray()
        self._send_buffer = bytearray()
        self._deferred_buffer = bytearray()
        self.sent_header_pos = 0

        self._stats = ConnStats()
        # self._to_send_metadata = collections.deque()
        self.pending_request_handles = []
        self._inflights = 0
        self._inflight_lbas = 0
        self._blens = []
        self._recv_pending = False
        self._registered = False
        self._next_req_time = time.time()
        self._writable = True
        for opt in self._sockopt:
            try:
                self._sock.setsockopt(socket.IPPROTO_TCP, opt, 1)
            except (OSError, NameError):
                logger.info(f"Got a wrong opt {opt}")


    cpdef set_SLO(self, IOPS_SLO=None, latency_us_SLO=None, rw_ratio=100):
        self.IOPS_SLO = IOPS_SLO
        if IOPS_SLO:
            self.fixed_interval = 1.0 / IOPS_SLO
        else:
            self.fixed_interval = 0  # closed-loop
        logger.debug(f"Fixed interval: {self.fixed_interval}")
        self.latency_us_SLO = latency_us_SLO
        self.rw_ratio = rw_ratio
        if self._qc_enabled:
            self._flow_enforcer.set_flow_target(IOPS_SLO, latency_us_SLO, rw_ratio)
        self.send_cmdreg()

    cpdef get_suggested_num(self):
        # outstanding_num = len(self._to_send_metadata)
        outstanding_num = len(self.pending_requests)
        if outstanding_num > SEND_MAX:
            self._stats.send_bloat_times += 1
        if self._fc_enabled:
            return self._flow_enforcer.get_next_batch(outstanding_num)
        else:
            if time.time() < self._next_req_time:
                return 0
            else:
                self._next_req_time += self.fixed_interval
                return 1

    cpdef connect(self, ip: str, port: int):
        # blocking connect
        if self._selector:
            self._selector = selectors.DefaultSelector()
        logger.info(f'Connecting {ip}:{port}')
        try:    
            self._sock.connect((ip, port))
        except TimeoutError:
            logger.error(f'Connection to {ip}:{port} timed out')
        if self._selector:
            self._sock.setblocking(False)
            self._selector.register(
                self._sock, selectors.EVENT_READ | selectors.EVENT_WRITE)

    cpdef poll(self):
        self._stats.poll_times += 1

        resp_len = 0
        if self._selector:
            if self._stats.to_send_bytes == 0 and self._stats.to_recv_bytes == 0:
                return
            if self._fc_enabled:
                if self._flow_enforcer.send_now():
                    self._writable = True
                else:
                    self._writable = False
            if self._writable:
                # if len(self.pending_request_handles):
                #     print(f"Pending request hanldes: {self.pending_request_handles}")
                #     print(f"Pending requests: {self.pending_requests}")
                if len(self.pending_request_handles) > self._inflights:
                # if len(self._to_send_metadata) > self._inflights:
                    handle = self.pending_request_handles[self._inflights]
                    to_send = self.pending_requests[handle][0]
                    # to_send = self._to_send_metadata[self._inflights][0]
                    for key, mask in self._selector.select(SELECTOR_TIMEOUT):
                        if self._stats.to_send_bytes and (mask & selectors.EVENT_WRITE):
                            sent = self._sock.send(self._send_buffer[:to_send])
                            self._send_buffer = self._send_buffer[sent:]
                            self._stats.to_send_bytes -= sent
                            if sent:
                                self._stats.tr_orders.append(sent)
                            # self._to_send_metadata[self._inflights][0] -= sent
                            self.pending_requests[handle][0] -= sent
                            # the first request fully sent
                            if sent == to_send:
                                self._inflights += 1
                                self._inflight_lbas += self.pending_requests[handle][4]
                                # if self._req_sent_cb:
                                #     self._req_sent_cb()
                if self._qc_enabled and (self._inflight_lbas >= self._max_inflights_lbas or self._inflights >= self._max_inflights):
                    self._writable = False
            for key, mask in self._selector.select(SELECTOR_TIMEOUT):
                if self._stats.to_recv_bytes and (mask & selectors.EVENT_READ):
                    resp_one = self._sock.recv(min(MAX_TO_RECV, self._stats.to_recv_bytes))
                    resp_len += len(resp_one)
                    self._stats.to_recv_bytes -= len(resp_one)
                    self._recv_buffer.extend(resp_one)
            # for key, mask in self._selector.select(SELECTOR_TIMEOUT):
            #    if len(self._deferred_buffer) and (mask & selectors.EVENT_WRITE):
            #        sent = self._sock.send(self._deferred_buffer)
            #        self._deferred_buffer = self._deferred_buffer[sent:]
            #        self._stats.to_send_bytes -= sent
        else:
            if self._stats.to_recv_bytes == 0:
                return
            resp = self._sock.recv(self._stats.to_recv_bytes)
            resp_len = len(resp)
            self._recv_buffer.extend(resp)
            self._stats.to_recv_bytes -= resp_len

        if resp_len:
            self._stats.tr_orders.append(-resp_len)
            self._stats.recv += 1
            self._stats.recv_bytes += resp_len
            if self._qc_enabled:
                if not self._writable and self._inflight_lbas < self._max_inflights_lbas and self._inflights < self._max_inflights:
                    self._writable = True
        else:
            return

        while len(self._recv_buffer) >= header_nbytes:
            if not self._recv_pending:
                # we have a new header
                recv_bytes = memoryview(bytes(self._recv_buffer[0:header_nbytes]))
                PyObject_GetBuffer(
                    recv_bytes, &buffer, PyBUF_SIMPLE | PyBUF_ANY_CONTIGUOUS)
                try:
                    self.recv_header = <binary_header_blk_t *> buffer.buf
                    self._recv_buffer = self._recv_buffer[header_nbytes:]
                    if self.recv_header.magic != header_nbytes:
                        logger.error('Wrong header received.')
                        return

                    if not self._registered and self.recv_header.opcode == CMD_REG:
                        response = self.recv_header.u2.resp_code
                        logger.debug(f'REG CMD {self.recv_header.opcode} returned')
                        if response == int.from_bytes(RESP_OK, "big"):
                            self._registered = True
                            logger.debug('Registration accepted')
                        elif response == int.from_bytes(RESP_FAIL, "big"):
                            logger.debug('Registration rejected')
                        else:
                            logger.info('Unknown registration failure')
                            raise Exception()
                    else:
                        handle = self.recv_header.req_handle

                        # if id(self._to_send_metadata[0]) == handle:
                        metadata = self.pending_requests[handle]
                        if metadata:
                            # _, buf, lba, lba_count = self._to_send_metadata.popleft()
                            _, buf, opcode, obj_size, lba_count = metadata
                            self.pending_request_handles.remove(handle)
                        else:
                            logger.error('Unknown request handle received')
                            # # unlikely, linear time lookup
                            # self._stats.OOO_times += 1
                            # if ENABLE_DEBUG:
                            #     logger.debug("Returned response out of order")
                            #     logger.info(f'Expect: {id(self._to_send_metadata[0])}')
                            #     logger.info(f'Revd: {handle}')
                            # for index in range(1, len(self._to_send_metadata)):
                            #     if id(self._to_send_metadata[index]) == handle:
                            #         _, buf, lba, lba_count = self._to_send_metadata[index]
                            #         assert (index < self._inflights), "This requests not sent"
                            #         del self._to_send_metadata[index]
                            #         break
                        self._inflights -= 1
                        self._inflight_lbas -= lba_count

                        if self._sca_enabled:
                            self._flow_enforcer.update_service_t(
                               self.recv_header.u1.service_time)

                        if self.recv_header.opcode == CMD_GET:
                            self._recv_pending = True
                            # can be saved for the next poll
                            self._app_buffer = buf
                            self._pending_bytes = lba_count * LBA_SIZE
                            # print(f"Get CMD sent: we have pending bytes {self._pending_bytes}")           
                        elif self.recv_header.opcode == CMD_SET:
                            response = self.recv_header.u2.resp_code
                            if self._fc_enabled:
                                self._flow_enforcer.recv_one()
                            # if self._req_done_cb:
                            #     self._req_done_cb()
                            self.completed_requests[handle] = None  # No data for write
                            # print(f"Set request {handle} completed with {response}")
                        else:
                            logger.error('Unknown opcode received')
                            return

                finally:
                    # clean up
                    PyBuffer_Release(&buffer)
            else:
                # we have a new (partial) payload
                if len(self._recv_buffer) >= self._pending_bytes:
                    self._app_buffer[:] = self._recv_buffer[:self._pending_bytes]
                    if self._fc_enabled:
                        self._flow_enforcer.recv_one()
                    # if self._req_done_cb:
                    #     self._req_done_cb(self._app_buffer)
                    handle = id(self._app_buffer)
                    self.completed_requests[handle] = self._app_buffer
                    self._recv_buffer = self._recv_buffer[self._pending_bytes:]
                    self._app_buffer = None
                    self._pending_bytes = 0
                    self._recv_pending = False
                else:
                    break

    cpdef dict get_completed_requests(self):
        """Retrieve and clear the dictionary of completed requests."""
        completed = self.completed_requests.copy()
        self.completed_requests.clear()
        return completed

    cpdef send_cmdreg(self):
        self.reg_header.magic = header_nbytes
        self.reg_header.opcode = CMD_REG
        self.reg_header.u1.SLO.IOPS_SLO = self.IOPS_SLO
        self.reg_header.u1.SLO.rw_ratio = self.rw_ratio
        self.reg_header.u1.SLO.latency_SLO_lo = self.latency_us_SLO % (1 << 16)
        self.reg_header.u1.SLO.latency_SLO_hi = self.latency_us_SLO >> 16
        self.reg_header.u2.token_demand = 0

        reg_header_bytes = <bytes>(<char *>self.reg_header)[:header_nbytes]

        self._sock.sendall(reg_header_bytes)

        self._stats.sent += 1
        self._stats.sent_bytes += header_nbytes
        self._stats.to_recv_bytes += header_nbytes

        if ENABLE_DEBUG:
            logger.debug(f'IOPS_SLO-{self.reg_header.u1.SLO.IOPS_SLO} '
                         f'rw_ratio-{self.reg_header.u1.SLO.rw_ratio} '
                         f'latency_us_SLO-{self.reg_header.u1.SLO.latency_SLO_lo}')
            logger.debug(f'Reg command sent: {reg_header_bytes}')

    cpdef send_cmdget(self, lba: int, lba_count: int, buf: bytearray, obj_size: int, deferred: bool):
        pos = self.sent_header_pos
        self.sent_header_pos = (self.sent_header_pos + 1) % SEND_HDR_DEPTH
        self.sent_headers[pos].magic = header_nbytes
        self.sent_headers[pos].opcode = CMD_GET
        self.sent_headers[pos].u1.lba = lba
        self.sent_headers[pos].u2.lba_count = lba_count
        # self._to_send_metadata.append([header_nbytes, buf, lba, lba_count])
        # self.sent_headers[pos].req_handle = id(self._to_send_metadata[-1])
        req_handle = id(buf)
        self.sent_headers[pos].req_handle = req_handle
        self.pending_requests[req_handle] = [header_nbytes, buf, CMD_GET, obj_size, lba_count]
        self.pending_request_handles.append(req_handle)
        # logger.info(f"Set request {req_handle} with {self.pending_requests[req_handle]} metadata")

        get_header_bytes = <bytes>(<char *>self.sent_headers[pos])[:header_nbytes]

        if self._selector:
            # for key, mask in self._selector.select(SELECTOR_TIMEOUT):
            #     if mask & selectors.EVENT_WRITE:
            #         sent = self._sock.send(self._send_buffer)
            #         self._send_buffer.extend(get_header_bytes[sent:])
            if deferred:
                self._deferred_buffer.extend(get_header_bytes)
            else:
                self._send_buffer.extend(get_header_bytes)
            self._stats.to_send_bytes += header_nbytes
        else:
            self._sock.sendall(get_header_bytes)

        if TRACE_BUF:
            self._blens.append(len(self._send_buffer))
        self._stats.sent += 1
        self._stats.sent_bytes += header_nbytes
        self._stats.to_recv_bytes += header_nbytes + lba_count * LBA_SIZE
        # print(f"Get request {req_handle} sent, pending bytes {self._stats.to_recv_bytes}")

    cpdef send_cmdset(self, lba: int, lba_count: int, buf: bytes, obj_size: int, deferred: bool):
        pos = self.sent_header_pos
        self.sent_header_pos = (self.sent_header_pos + 1) % SEND_HDR_DEPTH
        self.sent_headers[pos].magic = header_nbytes
        self.sent_headers[pos].opcode = CMD_SET
        self.sent_headers[pos].u1.lba = lba
        self.sent_headers[pos].u2.lba_count = lba_count

        # FIXME: this might generate identical req_handle for different requests
        req_handle = id(buf)
        self.sent_headers[pos].req_handle = req_handle
        to_send = header_nbytes + lba_count * LBA_SIZE
        self.pending_requests[req_handle] = [to_send, buf, CMD_SET, obj_size, lba_count]
        # print(f"Set request {req_handle} with {self.pending_requests[req_handle]} metadata")
        self.pending_request_handles.append(req_handle)
        # to_recv = header_nbytes + lba_count * LBA_SIZE
        # self._to_send_metadata.append([to_recv, buf, lba, lba_count])
        # self.sent_headers[pos].req_handle = id(self._to_send_metadata[-1])
        # assert(len(buf) == lba_count * LBA_SIZE)

        set_header_bytes = <bytes>(<char *>self.sent_headers[pos])[:header_nbytes]

        if self._selector:
            if deferred:
                self._deferred_buffer.extend(set_header_bytes)
                self._deferred_buffer.extend(buf)
            else:
                self._send_buffer.extend(set_header_bytes)
                self._send_buffer.extend(buf)
            self._stats.to_send_bytes += to_send
        else:
            self._sock.sendall(set_header_bytes)
            self._sock.sendall(buf)

        if TRACE_BUF:
            self._blens.append(len(self._send_buffer))
        self._stats.sent += 1
        self._stats.sent_bytes += header_nbytes + lba_count * LBA_SIZE
        self._stats.to_recv_bytes += header_nbytes

    cpdef get_traces(self):
        return self._blens

    cpdef close_connection(self):
        logger.debug(f"{self._stats}: finishing outstanding requests...")
        self._close_start_t = time.time()
        if self._selector:
            self._selector.modify(self._sock, selectors.EVENT_READ)
        while self._stats.to_recv_bytes > 0:
            self.poll()
            if (time.time() - self._close_start_t) >= CONN_CLOSE_TIMEOUT:
                logger.info("Connection closed with timeout")
                break
        if self._stats.recv:
            self._stats.avg_recv = self._stats.recv_bytes // self._stats.recv
        if self._stats.sent:
            self._stats.avg_sent = self._stats.sent_bytes // self._stats.sent
        # logger.info(f"{self._stats}")
        self._sock.close()
        if self._selector:
            self._selector.close()
        self.init_new() # for possible next connection
        # free(self.reg_header)
        # free(self.recv_header)
        # free(self.get_header)
        # free(self.set_header)

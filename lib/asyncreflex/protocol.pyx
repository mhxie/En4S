# cython: language_level=3
# cython: profile=False
# protocol.pyx
#
# Client protocol class

cimport cython

from cpython.buffer cimport PyObject_GetBuffer, PyBuffer_Release
from cpython.buffer cimport PyBUF_ANY_CONTIGUOUS, PyBUF_SIMPLE
from cpython.ref cimport PyObject
from libc.stdlib cimport malloc, free
# from libc.string cimport memcpy
# from libc.stdio cimport printf

# include "reflex.pxd"
include "consts.pxi"

import asyncio
import socket
import collections
import time

from asyncreflex.flow import FlowEnforcer
from asyncreflex.stats import ConnStats

import logging
logger = logging.getLogger(__name__)

cdef Py_buffer buffer
header_nbytes = sizeof(binary_header_blk_t)

cdef binary_header_blk_t *recv_header = <binary_header_blk_t *> malloc(header_nbytes)
cdef binary_header_blk_t *reg_header = <binary_header_blk_t *> malloc(header_nbytes)
cdef binary_header_blk_t *set_header = <binary_header_blk_t *> malloc(header_nbytes)
cdef binary_header_blk_t *get_header = <binary_header_blk_t *> malloc(header_nbytes)


class ReFlexClientProtocol(asyncio.Protocol):
    """ ReFlex client protocol class
    """

    def __init__(self, opts):
        self.loop = asyncio.get_running_loop()
        # self._can_write = asyncio.Event()
        # self._can_write.set()
        # self.handler = handler
        # self.cmd_queue = {}   # buffering sent commands
        # self._outstanding_reqs = collections.OrderedDict()
        self._outstanding_reqs = collections.deque()
        self._blens = []
        self._stats = ConnStats()
        self._req_done_cb = None
        self._registered = False
        self._recv_pending = False
        self._buffer = bytearray()  # buffering received bytes
        self._next_req_time = time.time()
        self._fc_enabled = False
        if "enable_fc" in opts:
            self._fc_enabled = True
        if self._fc_enabled:
            self._flow_enforcer = FlowEnforcer()

    def set_SLO(self, IOPS_SLO=None, latency_us_SLO=None, rw_ratio=100):
        self.IOPS_SLO = IOPS_SLO
        self.fixed_interval = 1.0 / IOPS_SLO
        logger.debug(f"Fixed interval: {self.fixed_interval}")
        self.latency_us_SLO = latency_us_SLO
        self.rw_ratio = rw_ratio
        if self._fc_enabled:
            self._flow_enforcer.set_flow_target(IOPS_SLO, latency_us_SLO, rw_ratio)
        self.send_cmdreg()

    def get_suggested_num(self):
        outstanding_num = len(self._outstanding_reqs)
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

    def connection_made(self, transport):
        self.transport = transport
        self.sock = transport.get_extra_info('socket')
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        wlimit = self.transport.get_write_buffer_limits()
        logger.info(f'Connection made: write limit is {wlimit} bytes.')
        if not self.loop:
            self.loop = asyncio.get_running_loop()
            if ENABLE_DEBUG:
                logger.debug("Protocol got asyncio running loop")

    def add__req_done_cb(self, callback):
        self._req_done_cb = callback

    def data_received(self, data):
        self._stats.recv_bytes += len(data)
        self._stats.recv += 1
        self._stats.to_recv_bytes -= len(data)
        self._buffer.extend(data)  # FIXME: avoid copy and concatenation

        while len(self._buffer) >= header_nbytes:
            if not self._recv_pending:
                # we have a new header
                recv_bytes = memoryview(bytes(self._buffer[0:header_nbytes]))
                PyObject_GetBuffer(
                    recv_bytes, &buffer, PyBUF_SIMPLE | PyBUF_ANY_CONTIGUOUS)
                try:
                    recv_header = <binary_header_blk_t *> buffer.buf
                    self._buffer = self._buffer[header_nbytes:]
                    if recv_header.magic != header_nbytes:
                        logger.error('Wrong header received.')
                        return

                    if not self._registered and recv_header.opcode == CMD_REG:
                        response = recv_header.u2.resp_code
                        logger.debug(f'REG CMD {recv_header.opcode} returned')
                        if response == int.from_bytes(RESP_OK, "big"):
                            self._registered = True
                            logger.debug('Registration accepted')
                        elif response == int.from_bytes(RESP_FAIL, "big"):
                            logger.debug('Registration rejected')
                        else:
                            logger.info('Unknown registration failure')
                            raise Exception()
                    else:
                        handle = recv_header.req_handle

                        # if ENABLE_DEBUG and first(self._outstanding_reqs) != handle:
                        #     logger.info('Outstanding requests are not in order.')
                        if id(self._outstanding_reqs[0]) == handle:
                            buf, lba, lba_count = self._outstanding_reqs.popleft()
                        else:
                            # unlikely, linear lookup
                            if ENABLE_DEBUG:
                                logger.debug("Returned response out of order")
                            for index in range(1, len(self._outstanding_reqs)):
                                if id(self._outstanding_reqs[index]) == handle:
                                    buf, lba, lba_count = self._outstanding_reqs[index]
                                    del self._outstanding_reqs[index]
                                    break

                        if self._fc_enabled:
                            self.enforcer.update_rtt(recv_header.u1.service_time)

                        if recv_header.opcode == CMD_GET:
                            self._recv_pending = True
                            self.recv_buffer = buf
                            self._pending_bytes = lba_count * LBA_SIZE
                        elif recv_header.opcode == CMD_SET:
                            response = recv_header.u2.resp_code
                            if self._req_done_cb:
                                self._req_done_cb()
                        else:
                            logger.error('Unknown opcode received')
                            return

                finally:
                    # clean up
                    PyBuffer_Release(&buffer)
            else:
                # we have a new (partial) payload
                if len(self._buffer) >= self._pending_bytes:
                    self.recv_buffer[:] = self._buffer[0:self._pending_bytes]
                    if self._req_done_cb:
                        self._req_done_cb(self.recv_buffer)
                    self._buffer = self._buffer[self._pending_bytes:]
                    self.recv_buffer = None
                    self._pending_bytes = 0
                    self._recv_pending = False
                else:
                    break

    def send_cmdreg(self):
        reg_header.magic = header_nbytes
        reg_header.opcode = CMD_REG
        reg_header.u1.SLO.IOPS_SLO = self.IOPS_SLO
        reg_header.u1.SLO.rw_ratio = self.rw_ratio
        reg_header.u1.SLO.latency_SLO_lo = self.latency_us_SLO % (1 << 16)
        reg_header.u1.SLO.latency_SLO_hi = self.latency_us_SLO >> 16
        reg_header.u2.token_demand = 0

        reg_header_bytes = <bytes>(<char *>reg_header)[:header_nbytes]

        self.transport.write(reg_header_bytes)

        self._stats.sent += 1
        self._stats.sent_bytes += header_nbytes
        self._stats.to_recv_bytes += header_nbytes

        if ENABLE_DEBUG:
            logger.debug(f'IOPS_SLO-{reg_header.u1.SLO.IOPS_SLO} '
                         f'rw_ratio-{reg_header.u1.SLO.rw_ratio} '
                         f'latency_us_SLO-{reg_header.u1.SLO.latency_SLO_lo}')
            logger.debug(f'Reg command sent: {reg_header_bytes}')

    def send_cmdset(self, lba, lba_count, buf, deferred):
        set_header.magic = header_nbytes
        set_header.opcode = CMD_SET
        set_header.u1.lba = lba
        set_header.u2.lba_count = lba_count

        self._outstanding_reqs.append((buf, lba, lba_count))

        set_header.req_handle = id(self._outstanding_reqs[-1])

        set_header_bytes = <bytes>(<char *>set_header)[:header_nbytes]

        self.transport.write(set_header_bytes)
        self.transport.write(buf)

        # self._outstanding_reqs[lba] = (buf, lba, lba_count)

        if TRACE_BUF:
            # FIXME: this is not correct, but it's okay for now
            self._blens.append(lba_count * LBA_SIZE + header_nbytes)
        self._stats.sent += 1
        self._stats.sent_bytes += header_nbytes + lba_count * LBA_SIZE
        self._stats.to_recv_bytes += header_nbytes

    def send_cmdget(self, lba, lba_count, buf, deferred):
        get_header.magic = header_nbytes
        get_header.opcode = CMD_GET
        get_header.u1.lba = lba
        get_header.u2.lba_count = lba_count

        self._outstanding_reqs.append((buf, lba, lba_count))

        get_header.req_handle = id(self._outstanding_reqs[-1])
        # get_header.req_handle = id(buf) # must be unique
        get_header_bytes = <bytes>(<char *>get_header)[:header_nbytes]

        self.transport.write(get_header_bytes)
        # self._outstanding_reqs[id(buf)] = (buf, lba, lba_count)

        if TRACE_BUF:
            # FIXME: this is not correct, but it's ok for now
            self._blens.append(header_nbytes)
        self._stats.sent += 1
        self._stats.sent_bytes += header_nbytes
        self._stats.to_recv_bytes += header_nbytes + lba_count * LBA_SIZE

    def pause_writing(self) -> None:
        # self._can_write.clear()
        pass

    def resume_writing(self) -> None:
        # self._can_write.set()
        pass

    def get_traces(self):
        return self._blens

    async def close_connection(self):
        while len(self._outstanding_reqs) > 0:
            # print(f'Waiting for {len(self._outstanding_reqs)} pending requests')
            await asyncio.sleep(0)

        if self.transport.can_write_eof():
            if ENABLE_DEBUG:
                logger.debug('Sending EOF')
            self.transport.write_eof()
            if ENABLE_DEBUG:
                logger.debug('EOF sent')
            self.transport.close()
            self._stats.avg_recv = self._stats.recv_bytes // self._stats.recv
            self._stats.avg_sent = self._stats.sent_bytes // self._stats.sent
            logger.info(f"{self._stats}")

    def connection_lost(self, exc):
        if exc is None:
            logger.info("Connection closed successfully.")
        else:
            logger.info("The server connection full.")

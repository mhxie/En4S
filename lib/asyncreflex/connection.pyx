# cython: language_level=3
# cython: profile=False
# connection.pyx
#
# AsyncReFlex Controller connection Class and Client Connection Class

include "consts.pxi"
include "reflex.pxd"

from asyncreflex.protocol import ReFlexClientProtocol
from asyncreflex.socket import ReFlexSocket
from asyncreflex.context import ReFlexContext
from asyncreflex.message import ContextMsg, AllocateMsg, DeallocateMsg, RegisterMsg, DeregisterMsg, ContextMsg

import asyncio
import socket
import os
import json
from math import ceil
import ctypes

import logging
logger = logging.getLogger(__name__)

cdef short header_nbytes = sizeof(binary_header_blk_t)


class ControllerConnection:
    """A connection class to access controller"""

    def __init__(self, controller_addr, handler, loop=None):
        addr = controller_addr.split(':')
        self.ip = addr[0]
        self.port = int(addr[1])
        self.handler = handler
        self.jid = None
        if loop:
            self.loop = loop
        else:
            self.loop = asyncio.get_event_loop()

    async def __aenter__(self):
        """Connect to the target controller when entering the with scope"""
        logger.debug("Entering the controller connection")
        await self.connect()
        return self

    async def __aexit__(self, *args, **kwargs):
        """Close the connection when exiting the with scope"""
        logger.debug("Exiting the controller connection")
        await self.close()

    async def connect(self):
        logger.debug('Connecting to the controller...')
        self.reader, self.writer = await asyncio.open_connection(
            self.ip, self.port)
        logger.debug('Controller connection established')

    async def allocate_capacity(self, hash_name: int, job) -> ReFlexContext:
        self.writer.write(CTRL_ALLOC)
        logger.debug(f'Sent CMD {CTRL_ALLOC} to the controller')

        # Create an instance of AllocateMsg using ctypes
        allocate_msg = AllocateMsg()
        allocate_msg.app_name = hash_name
        allocate_msg.req_size = job.req_size
        allocate_msg.IOPS_SLO = job.IOPS_SLO
        allocate_msg.latency_us_SLO = job.latency_us_SLO
        allocate_msg.rw_ratio = job.rw_ratio
        allocate_msg.concurrency = job.concurrency
        allocate_msg.capacity = job.capacity  # capacity in LBAs

        # Serialize the ctypes.Structure to bytes
        msg_bytes = ctypes.string_at(
            ctypes.addressof(allocate_msg), ctypes.sizeof(allocate_msg)
        )
        self.writer.write(msg_bytes)
        await self.writer.drain()
        ack = await self.reader.readexactly(1)

        if ack == RESP_OK:
            ctx_size = ctypes.sizeof(ContextMsg)
            logger.debug(f"Waiting for the context (len-{ctx_size})")
            ctx_bytes = await self.reader.readexactly(ctx_size)
            ctx_struct = ContextMsg()
            ctypes.memmove(
                ctypes.addressof(ctx_struct), ctx_bytes, ctx_size
            )
            ctx = ReFlexContext(raw_ctx=ctx_struct)
            logger.debug(f"Got ctx bytes: {ctx_bytes} (size: {len(ctx_bytes)})")
            if ctx.metadata_size:
                metadata_bytes = await self.reader.readexactly(ctx.metadata_size)
                ctx.merge_metadata(metadata_bytes)
                if ENABLE_DEBUG:
                    logger.debug(f"Got metadata {ctx.metadata}")
            self.jid = ctx.jid
            logger.debug(f"Got capacity {ctx.cap / 2} KiB allocated")
            return ctx
        else:
            logger.debug(f"Allocation failed with ack {ack}")

    async def free_capacity(self):
        await self.connect()
        self.writer.write(CTRL_DEALLOC)
        logger.debug(f'Sent CMD {CTRL_DEALLOC} to the controller')

        # Create an instance of DeallocateMsg using ctypes
        deallocate_msg = DeallocateMsg()
        deallocate_msg.jid = self.jid

        msg_bytes = ctypes.string_at(
            ctypes.addressof(deallocate_msg), ctypes.sizeof(deallocate_msg)
        )
        self.writer.write(msg_bytes)
        await self.writer.drain()
        ack = await self.reader.readexactly(1)
        if ack == RESP_OK:
            logger.debug("Deallocation succeeded")
        else:
            logger.debug(f"Deallocation failed with ack {ack}")
        await self.close()

    async def register_flow(self, flow):
        """Register a new flow at controller"""
        self.flow_id = self.compute_hash(flow)
        await self.connect()
        self.writer.write(CTRL_REG)

        # Create an instance of RegisterMsg using ctypes
        register_msg = RegisterMsg()
        register_msg.jid = self.jid
        register_msg.IOPS_SLO = flow.IOPS_SLO
        register_msg.latency_us_SLO = flow.latency_us_SLO
        register_msg.rw_ratio = flow.rw_ratio
        register_msg.req_size = flow.req_size
        register_msg.sequential = flow.sequential
        register_msg.persistent = flow.persistent

        msg_bytes = ctypes.string_at(
            ctypes.addressof(register_msg), ctypes.sizeof(register_msg)
        )
        logger.debug(f"Sent CMD {CTRL_REG} to register {flow}")
        logger.debug(f"jid-{register_msg.jid}, IOPS_SLO-{register_msg.IOPS_SLO}")
        logger.debug(f"Sending policy bytes {msg_bytes} (len {ctypes.sizeof(register_msg)})")
        self.writer.write(msg_bytes)
        await self.writer.drain()
        ack = await self.reader.readexactly(1)
        if ack == RESP_OK:
            logger.debug("Registration succeeded")
        else:
            logger.debug(f"Registration failed with ack {ack}")
        await self.close()

    async def deregister_job(self):
        await self.connect()
        self.writer.write(CTRL_DEREG)

        # Create an instance of DeregisterMsg using ctypes
        deregister_msg = DeregisterMsg()
        deregister_msg.jid = self.jid
        deregister_msg.fid = self.flow_id

        msg_bytes = ctypes.string_at(
            ctypes.addressof(deregister_msg), ctypes.sizeof(deregister_msg)
        )
        self.writer.write(msg_bytes)
        logger.debug(f"Sent CMD {CTRL_DEREG} to deregister {msg_bytes}")
        await self.writer.drain()
        ack = await self.reader.readexactly(1)
        if ack == RESP_OK:
            logger.debug("Deregistration succeeded")
        else:
            logger.debug(f"Deregistration failed with ack {ack}")
        await self.close()

    async def update_context(self, context_msg_bytes, metadata_bytes):
        """Upload the context to the controller"""
        await self.connect()
        self.writer.write(CTRL_UPDATE)
        self.writer.write(context_msg_bytes)
        await self.writer.drain()
        self.writer.write(metadata_bytes)
        await self.writer.drain()
        if ENABLE_DEBUG:
            logger.debug(f"Sending new context {new_ctx}")
        resp = await self.reader.readexactly(1)  # Response code
        await self.close()
        if resp == RESP_OK:
            return True
        return False

    async def close(self):
        """Close the connection to the remote"""
        logger.debug('Closing the controller connection...')
        self.writer.close()
        await self.writer.wait_closed()
        logger.debug('Controller connection closed')

    def compute_hash(self, policy):
        """Same hash function applied at controller"""
        value = (policy.IOPS_SLO << 32) | (policy.rw_ratio << 24) | policy.latency_us_SLO
        return value

    async def flush_context(self):
        """Flush the context to the controller"""
        ctx_msg_bytes, metadata_bytes = self.handler.state_ctx.flush()
        ret = await self.update_context(ctx_msg_bytes, metadata_bytes)  # Flush the new context
        if ret:
            logger.debug("Context flushed successfully")
        else:
            logger.error("Context flush failed")

class ReFlexConnection:
    """A long-running ReFlex connection class with high-level storage APIs"""

    async def __aenter__(self):
        """Connect to the target storage when entering with scope"""
        logger.debug("Entering the ReFlex connection")
        if self._ctx:
            addr = self._ctx.get_addr()
            self.ip, self.port = addr.split(':')
            self.port = int(self.port)
            await self.connect_storage(self.ip, self.port)
        return self

    async def __aexit__(self, *args, **kwargs):
        """Flush the results and close the connection when exiting the with scope"""
        logger.debug('Closing the storage connection...')
        await self.close()
        # we have flush_context() in ControllerConnection called back
        for callback in self.__exit_cb:
            await callback()
        self.__exit_cb = []
        logger.debug('Storage connection closed')

    def __init__(self, handler, loop, use_sock, user_opts, boto_cli, send_max=SEND_MAX):
        self.handler = handler
        self._ctx = self.handler.state_ctx
        self.__exit_cb = []
        self.__buffer_size = 262144 * send_max
        self.__buffer = bytearray(self.__buffer_size)
        self.__pos = 0
        if loop:
            self.loop = loop
        else:
            self.loop = asyncio.get_event_loop()

        self.delegator = boto_cli
        self.delegator_name = os.getenv('delegator')
        self.delegation_count = 0

        self.use_sock = use_sock
        self.conn = None

        self.req_done_cbs = []

        if self.use_sock:
            opts = {"sockopts": [], "send_max": send_max}
            if "NODELAY" in user_opts:
                opts["sockopts"].append(socket.TCP_NODELAY)
            if "QUICKACK" in user_opts:
                opts["sockopts"].append(socket.TCP_QUICKACK)
            if "NONBLOCKING" in user_opts:
                opts["nonblocking"] = None
            if "enable_qc" in user_opts:
                opts["enable_fc"] = 1
            if "enable_fc" in user_opts:
                opts["enable_fc"] = 2
            if "enable_sca" in user_opts:
                opts["enable_fc"] = 3
            self.sock = ReFlexSocket(opts)
        else:
            self.proto = ReFlexClientProtocol(user_opts)

    def set_req_done_cb(self, callback):
        """Add a callback to be called when a request is completed."""
        self.req_done_cb = callback

    def remove_req_done_cb(self):
        """Remove a previously added callback."""
        self.req_done_cb = None

    def add_exit_callback(self, callback):
        """Add a callback to be called upon exiting the context."""
        self.__exit_cb.append(callback)

    async def connect_storage(self, ip: str, port: int):
        """Connect to allocated storage node, only supports one for now"""
        if self.use_sock:
            self.sock.connect(ip, port)
            self.conn = self.sock
        else:
            self.transport, self.conn = await self.loop.create_connection(
                lambda: self.proto, ip, port
            )

    async def setup_new_flow(self, flow=None):
        """Set up a new flow at storage node"""
        if not flow:
            flow.IOPS_SLO = 0
            flow.latency_us_SLO = 0
            flow.rw_ratio = 100
        logger.debug(f"Setting up new flow at storage: IOPS_SLO-{flow.IOPS_SLO} "
                     f"latency_SLO-{flow.latency_us_SLO} "
                     f"rw_ratio-{flow.rw_ratio}")

        if self.use_sock:
            self.sock.set_SLO(
                flow.IOPS_SLO,
                flow.latency_us_SLO,
                flow.rw_ratio)
        else:
            self.proto.set_SLO(
                flow.IOPS_SLO,
                flow.latency_us_SLO,
                flow.rw_ratio)

    def put_object(self, obj, obj_name, deferred=False, delegate=False):
        """Put object bytes to the storage with specified name"""
        obj_size = len(obj)

        if self._ctx.exists(obj_name):
            obj_id, lba, lba_count, stored_obj_size = self._ctx.get_object_meta(obj_name)
            logger.debug(f"Warning: Object {obj_name} already exists, id {obj_id}")
        else:
            lba = self._ctx.get_next_avail_lba()
            if not self._ctx.update_object(obj_name, lba, obj_size):
                logger.error("Write fails, not enough space remaining")
                return
            obj_id, lba, lba_count, stored_obj_size = self._ctx.get_object_meta(obj_name)
            if ENABLE_DEBUG:
                logger.debug(f"Size of obj is {obj_size}, lba_count is {lba_count}")

        if delegate:
            # Delegated access (not modified)
            self._req_sent_cb()
            header = {
                'ip': self.ip,
                'port': self.port,
                'opcode': CMD_SET,
                'lba': lba,
                'lba_count': lba_count,
                'obj': obj,
            }
            self.delegate_access(header)
            self._req_done_cb()
        else:
            # Pad the object to the required size if necessary
            padded_data = obj.ljust(lba_count * LBA_SIZE, b'\x00')
            if ENABLE_DEBUG:
                logger.debug(f"Sent buf = {id(padded_data)}")
            # Send the command
            self.conn.send_cmdset(lba, lba_count, padded_data, obj_size, deferred)

    def get_object(self, obj_name: str, deferred=False, delegate=False):
        """Initiate a GET request for the object, non-blocking."""
        if not self._ctx.exists(obj_name):
            logger.error(f"Object {obj_name} does not exist")
            return

        _, lba, lba_count, obj_size = self._ctx.get_object_meta(obj_name)
        end_pos = self.__pos + lba_count * LBA_SIZE
        if end_pos > self.__buffer_size:
            logger.debug(f"Cyclic buffer all used, moving to the beginning")
            self.__pos = 0
            end_pos = lba_count * LBA_SIZE
        allocated_buf = self.__buffer[self.__pos:end_pos]

        if delegate:
            # Delegated access (not modified)
            self._req_sent_cb()
            header = {
                'ip': self.ip,
                'port': self.port,
                'opcode': CMD_GET,
                'lba': lba,
                'lba_count': lba_count,
                'obj': None,
            }
            self.delegate_access(
                header,
                allocated_buf
            )
            self.req_done_cb(allocated_buf)
        else:
            if ENABLE_DEBUG:
                logger.debug(f"Sent buf = {id(allocated_buf)}")
            self.conn.send_cmdget(lba, lba_count, allocated_buf, obj_size, deferred)

        self.__pos = end_pos
        return allocated_buf[:obj_size]

    def poll(self):
        """Poll the connection for completed requests."""
        self.conn.poll()

        # Process completed requests
        completed_requests = self.conn.get_completed_requests()
        for req_handle, response in completed_requests.items():
            metadata = self.conn.pending_requests.pop(req_handle, None)
            if metadata:
                _, buf, opcode, obj_size, lba_count = metadata
                logger.debug(f"Received buf = {id(buf)}")
                if opcode == CMD_GET:
                    self.req_done_cb(buf[:obj_size])
                elif opcode == CMD_SET:
                    # Write operation completed
                    self.req_done_cb(None)
                else:
                    logger.error(f"Unknown opcode received, got {opcode}/{obj_size}")
            else:
                logger.error("Received response for unknown request handle")

    async def close(self):
        if self.use_sock:
            self.conn.close_connection()
        else:
            await self.conn.close_connection()

    def load_encrypted_context(self):
        pass

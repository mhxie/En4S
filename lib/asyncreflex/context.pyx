# cython: language_level=3
# cython: profile=False
# context.pyx

include "consts.pxi"

import json
import logging
import hashlib
import ctypes
from asyncreflex.message import ContextMsg

logger = logging.getLogger(__name__)
cdef int context_count = 0

cdef class ReFlexContext:
    """
    A structure that stores metadata to map object IDs to logical addresses,
    improved to handle exact object sizes.
    """
    cdef public list ip
    cdef public unsigned int jid
    cdef public unsigned int port
    cdef public unsigned int cap
    cdef public unsigned long next_lba
    cdef public unsigned long metadata_size
    cdef public unsigned long first_lba
    cdef public unsigned long last_lba
    cdef public unsigned long app_name
    cdef public dict metadata


    def __init__(self, raw_ctx=None, cap=1024, name=0, invoker_state=None):
        global context_count
        if ENABLE_DEBUG:
            logger.debug(f'Initializing ReFlexContext with {raw_ctx!r}')
        self.metadata = {}

        # context from contoller
        if raw_ctx:
            ctx = raw_ctx  # Use the passed ContextMsg instance directly

            self.ip = list(ctx.ip)
            self.port = ctx.port
            self.jid = ctx.jid
            self.next_lba = ctx.next_lba
            self.cap = ctx.capacity
            self.metadata_size = ctx.metadata_size
            self.first_lba = ctx.first_lba
            self.last_lba = ctx.last_lba
            self.app_name = ctx.app_name
            # if metadata_size is not emtpy, expecting to recive metadata to load
            # then a subsequent merge_metadata will be called          
        # context from invoker
        elif invoker_state:
            self.merge_state_with_curr(invoker_state, is_invokee=True)
        else:
            # Dummy context for testing
            self.jid = context_count
            context_count += 1
            self.ip = [127, 0, 0, 1]
            self.port = 8888
            self.next_lba = 0
            self.cap = cap
            self.first_lba = 0
            self.last_lba = cap - 1
            self.app_name = name

    def merge_metadata(self, metadata_bytes):
        new_metadata = json.loads(metadata_bytes.decode())
        if new_metadata:
            self.metadata = self.metadata | new_metadata

    def generate_dummy_metadata(self, obj_size):
        num_block = (obj_size + LBA_SIZE - 1) // LBA_SIZE  # Ceiling division
        num_obj = self.cap // num_block
        for i in range(num_obj):
            obj_name = f"{obj_size}_{i}"
            if ENABLE_DEBUG:
                logger.debug(f'Inserting {obj_name}')
            assert self.update_object(obj_name, self.next_lba, obj_size)

    def update_scheduled_ctx(self, job_ctx, snode):
        ip, port = snode.addr.split(':')
        self.set_target_storage(ip, port)
        self.IOPS_SLO = job_ctx.IOPS_SLO
        self.latency_us_SLO = job_ctx.latency_us_SLO

    def get_id(self):
        return self.jid

    def get_addr(self):
        ip_str = [str(num) for num in self.ip]
        ip = '.'.join(ip_str)
        return f"{ip}:{self.port}"

    def set_target_storage(self, addr_ip: str, addr_port: int):
        self.ip = [int(part) for part in addr_ip.split('.')]
        self.port = int(addr_port)

    def get_next_avail_lba(self):
        return self.next_lba

    def exists(self, obj_name: str):
        obj_id = self.hash(obj_name)
        return str(obj_id) in self.metadata

    def get_object_meta(self, obj_name: str):
        obj_id = self.hash(obj_name)
        obj_meta = self.metadata[str(obj_id)]
        return obj_id, obj_meta[1], obj_meta[2], obj_meta[3]  # lba, lba_count, obj_size

    def update_object(self, obj_name: str, lba: int, obj_size: int):
        # fixme, we don't support larger sized objects
        obj_id = self.hash(obj_name)
        lba_count = (obj_size + LBA_SIZE - 1) // LBA_SIZE  # Ceiling division

        if str(obj_id) in self.metadata:
            existing_meta = self.metadata[str(obj_id)]
            assert existing_meta[0] == obj_name, (
                "Collision detected. Please use a better hash function."
            )
            self.metadata[str(obj_id)] = (obj_name, lba, lba_count, obj_size)
            return True
        elif self.next_lba + lba_count <= self.last_lba:
            self.metadata[str(obj_id)] = (obj_name, lba, lba_count, obj_size)
            self.next_lba += lba_count
            return True
        else:
            if ENABLE_DEBUG:
                logger.debug(
                    f'Cannot update object {obj_name}->{obj_id}, '
                )
            return False

    def hash(self, id_str: str):
        # Use SHA-256 hash function for better collision avoidance
        h = hashlib.sha256(id_str.encode('utf-8')).digest()
        return int.from_bytes(h[:8], 'big')  # Use first 8 bytes for a 64-bit integer

    def reserve_cap(self, re_cap):
        if self.next_lba + re_cap <= self.cap:
            self.next_lba += re_cap
            return True
        else:
            logger.error(f'Cannot reserve {re_cap} blocks')
            return False

    def flush(self):
        # Create an instance of ContextMsg
        ctx = ContextMsg()
        for i in range(4):
            ctx.ip[i] = self.ip[i]
        ctx.port = self.port
        ctx.jid = self.jid
        ctx.next_lba = self.next_lba
        ctx.capacity = self.cap
        ctx.first_lba = self.first_lba
        ctx.last_lba = self.last_lba
        ctx.app_name = self.app_name

        # Handle metadata_size and the rest of the code
        metadata_bytes = json.dumps(self.metadata).encode()
        # only update when flushed
        ctx.metadata_size = len(metadata_bytes)
        if ENABLE_DEBUG:
            logger.debug(f'Metadata size is {ctx.metadata_size}')

        # Serialize the ContextMsg to bytes
        ctx_bytes = ctypes.string_at(ctypes.addressof(ctx), ctypes.sizeof(ctx))
        return ctx_bytes, metadata_bytes

    def get_invokee_state(self, re_cap=0):
        state_dict = {
            'ip': self.ip,
            'port': self.port,
            'jid': self.jid,
            'next_lba': self.next_lba,
            'cap': self.cap,
            'metadata_size': self.metadata_size,
            'first_lba': self.first_lba,
            'last_lba': self.last_lba,
            'app_name': self.app_name,
            'metadata': self.metadata,
        } 
        if re_cap > 0:
            # state is for invokee, reserve capacity for it
            state_dict['last_lba'] = self.next_lba + re_cap - 1
            # local ctx or other invokee should not access the reserved region
            self.reserve_cap(re_cap = re_cap)
            return state_dict
        else:
            # state is for invoker
            return state_dict

    def merge_state_with_curr(self, state, is_invokee=False):
        self.ip = state['ip']
        self.port = state['port']
        self.jid = state['jid']
        self.cap = state['cap']
        self.app_name = state['app_name']
        if state['metadata']:
            self.metadata = state['metadata'] | self.metadata
        self.metadata_size = state['metadata_size'] # not updated
        self.next_lba = state['next_lba'] # usually the one get merged is larger
        if is_invokee:
            self.last_lba = state['last_lba']
        else:
            self.last_lba = self.cap - 1
        # if needed, run garbage collection to free up space
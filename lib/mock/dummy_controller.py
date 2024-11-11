#!/usr/bin/env python

import asyncio
import logging
import ctypes
import sys
from asyncreflex.context import ReFlexContext
from asyncreflex import JobCtx, FlowCtx
from asyncreflex.message import (
    AllocateMsg,
    DeallocateMsg,
    RegisterMsg,
    DeregisterMsg,
    ContextMsg,
)
from reflex import (
    RESP_OK,
    RESP_FAIL,
    CTRL_ALLOC,
    CTRL_DEALLOC,
    CTRL_REG,
    CTRL_DEREG,
    CTRL_UPDATE,
)

# Configuration
ENABLE_METADATA = True
DESIRED_CONCURRENCY = 1

# Logging Setup
logger = logging.getLogger("controller")
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

g_contexts = {}  # Job ID to context mapping

# Scheduler Class
class SimpleScheduler:
    def __init__(self):
        self.flows = {}     # Flow hash to weighted IOPS mapping
        self.flow_counts = {}  # Flow hash to count mapping
        self.total_flow_counts = 0
        self.apps = {}  # App name to {'wIOPS': int, 'counts': int}
        self.storages = {
            "127.0.0.1:8888": {
                "capacity": 2 * 1024 * 1024 * 1024,  # in bytes
                "wIOPS": 1_000_000,
            },
            # Additional storage nodes can be added here
        }
        self.storage_addrs = list(self.storages.keys())
        self.round_robin = self._round_robin_generator()

    def _round_robin_generator(self):
        while True:
            for addr in self.storage_addrs:
                yield addr

    def dummy_schedule(self, app_name: int, job_ctx: JobCtx):
        if app_name not in self.apps:
            self.apps[app_name] = {"wIOPS": 0, "counts": 0}

        app_info = self.apps[app_name]
        estimated_wIOPS = app_info["wIOPS"] / app_info["counts"] if app_info["counts"] else 0

        for _ in range(len(self.storage_addrs)):
            addr = next(self.round_robin)
            node = self.storages[addr]
            if estimated_wIOPS < node["wIOPS"]:
                context = ReFlexContext(cap=job_ctx.capacity, name=app_name)
                ip, port = addr.split(":")
                context.set_target_storage(ip, int(port))
                g_contexts[context.jid] = context
                app_info["counts"] += 1
                # Return context data and metadata bytes
                context_data, meta_bytes = context.flush()
                return context_data, meta_bytes
        logger.warning("No suitable storage node found for allocation")
        return None, None


    def free_capacity(self, jid):
        if jid in g_contexts:
            addr = g_contexts[jid].get_addr()
            self.storages[addr]["capacity"] += g_contexts[jid].cap
            del g_contexts[jid]
            logger.info(f"Freed capacity for Job ID {jid}")
        else:
            logger.warning(f"Job ID {jid} not found in contexts")

    def register_flow(self, flow_jid, flow_ctx):
        flow_hash = self.compute_hash(flow_ctx)
        flow_wIOPS = self.compute_cost(flow_ctx)
        jid = flow_jid
        app_name = g_contexts[jid].app_name

        self.flows[flow_hash] = flow_wIOPS
        self.flow_counts[flow_hash] = self.flow_counts.get(flow_hash, 0) + 1
        self.total_flow_counts += 1

        app_info = self.apps.get(app_name)
        if app_info:
            app_info["wIOPS"] += flow_wIOPS
            # FIXME, here we limit wIOPS per app not by the job limit
            storage_addr = g_contexts[jid].get_addr()
            self.storages[storage_addr]["wIOPS"] -= flow_wIOPS
            logger.info(f"Registered flow {flow_hash} for app {app_name}")
            return flow_hash
        else:
            logger.warning(f"App {app_name} not found during flow registration")
            return None

    def deregister_flow(self, jid, flow_hash):
        if flow_hash in self.flows and jid in g_contexts:
            flow_wIOPS = self.flows[flow_hash]
            storage_addr = g_contexts[jid].get_addr()
            self.storages[storage_addr]["wIOPS"] += flow_wIOPS
            self.flow_counts[flow_hash] -= 1
            self.total_flow_counts -= 1

            app_name = g_contexts[jid].app_name
            app_info = self.apps.get(app_name)
            if app_info:
                app_info["wIOPS"] -= flow_wIOPS
                logger.info(f"Deregistered flow {flow_hash} for app {app_name}")
            else:
                logger.warning(f"App {app_name} not found during flow deregistration")

            if self.flow_counts[flow_hash] <= 0:
                del self.flows[flow_hash]
                del self.flow_counts[flow_hash]
        else:
            logger.warning(f"Flow hash {flow_hash} or Job ID {jid} not found during deregistration")

    def compute_hash(self, policy):
        value = (policy.IOPS_SLO << 32) | (policy.rw_ratio << 24) | policy.latency_us_SLO
        return value

    def compute_cost(self, policy):
        # Implement any cost calculation logic here
        return policy.IOPS_SLO

# Global Scheduler Instance
scheduler = SimpleScheduler()

async def handle_client(reader, writer):
    try:
        opcode = await reader.readexactly(1)
        if opcode == CTRL_REG:
            await handle_register(reader, writer)
        elif opcode == CTRL_DEREG:
            await handle_deregister(reader, writer)
        elif opcode == CTRL_ALLOC:
            await handle_allocate(reader, writer)
        elif opcode == CTRL_DEALLOC:
            await handle_deallocate(reader, writer)
        elif opcode == CTRL_UPDATE:
            await handle_update(reader, writer)
        else:
            logger.error(f"Unknown opcode received: {opcode}")
            writer.write(RESP_FAIL)
            await writer.drain()
    except asyncio.IncompleteReadError:
        logger.info("Connection closed by client")
    # except Exception as e:
    #     logger.error(f"Error handling client: {e}")
    finally:
        writer.close()
        await writer.wait_closed()

async def handle_register(reader, writer):
    msg_size = ctypes.sizeof(RegisterMsg)
    raw_msg = await reader.readexactly(msg_size)
    msg = RegisterMsg()
    ctypes.memmove(ctypes.addressof(msg), raw_msg, msg_size)

    # Convert to FlowCtx
    flow_ctx = FlowCtx(
        IOPS_SLO=msg.IOPS_SLO,
        latency_us_SLO=msg.latency_us_SLO,
        req_size=msg.req_size,
        rw_ratio=msg.rw_ratio,
        sequential=bool(msg.sequential),
        persistent=bool(msg.persistent),
    )

    fid = scheduler.register_flow(msg.jid, flow_ctx)
    if fid:
        # Wait until desired concurrency is reached
        while scheduler.total_flow_counts < DESIRED_CONCURRENCY:
            logger.info("Waiting for desired concurrency")
            await asyncio.sleep(0)
        writer.write(RESP_OK)
        logger.info(f"Flow {fid} registered successfully")
    else:
        writer.write(RESP_FAIL)
        logger.warning("Flow registration failed")
    await writer.drain()

async def handle_deregister(reader, writer):
    msg_size = ctypes.sizeof(DeregisterMsg)
    raw_msg = await reader.readexactly(msg_size)
    msg = DeregisterMsg()
    ctypes.memmove(ctypes.addressof(msg), raw_msg, msg_size)

    scheduler.deregister_flow(msg.jid, msg.fid)
    writer.write(RESP_OK)
    await writer.drain()
    logger.info(f"Flow {msg.fid} deregistered for Job ID {msg.jid}")

async def handle_allocate(reader, writer):
    msg_size = ctypes.sizeof(AllocateMsg)
    raw_msg = await reader.readexactly(msg_size)
    msg = AllocateMsg()
    ctypes.memmove(ctypes.addressof(msg), raw_msg, msg_size)

    app_name = msg.app_name

    # Construct JobCtx from received message
    job_ctx = JobCtx(
        capacity=msg.capacity,
        IOPS_SLO=msg.IOPS_SLO,
        latency_us_SLO=msg.latency_us_SLO,
        rw_ratio=msg.rw_ratio,
        req_size=msg.req_size,
        concurrency=msg.concurrency,
        num_regions=msg.num_regions,
        encoded_DAG_len=msg.encoded_DAG_len,
    )

    sizeMB = job_ctx.capacity // 2 // 1024
    logger.info(f"Allocating {job_ctx.capacity} lbas ({sizeMB} MiB) for app {job_ctx.app_name}")
    logger.info(f"App Name: {app_name}, type: {type(app_name)}")
    context_data, meta_bytes = scheduler.dummy_schedule(app_name, job_ctx)
    if context_data is not None:
        writer.write(RESP_OK)
        writer.write(context_data)
        if ENABLE_METADATA and meta_bytes:
            writer.write(meta_bytes)
        await writer.drain()
        logger.info(f"Allocation successful for app {job_ctx.app_name}")
    else:
        writer.write(RESP_FAIL)
        await writer.drain()
        logger.warning(f"Allocation failed for app {job_ctx.app_name}")


async def handle_deallocate(reader, writer):
    msg_size = ctypes.sizeof(DeallocateMsg)
    raw_msg = await reader.readexactly(msg_size)
    msg = DeallocateMsg()
    ctypes.memmove(ctypes.addressof(msg), raw_msg, msg_size)

    scheduler.free_capacity(msg.jid)
    writer.write(RESP_OK)
    await writer.drain()
    logger.info(f"Deallocated resources for Job ID {msg.jid}")

async def handle_update(reader, writer):
    msg_size = ctypes.sizeof(ContextMsg)
    raw_msg = await reader.readexactly(msg_size)
    msg = ContextMsg()
    ctypes.memmove(ctypes.addressof(msg), raw_msg, msg_size)
    if msg.metadata_size > 0:
        metadata_bytes = await reader.readexactly(msg.metadata_size)
        logger.debug(f"Received metadata: {len(metadata_bytes)} bytes")
    if msg.jid in g_contexts:
        g_contexts[msg.jid].merge_metadata(metadata_bytes)
    else:
        logger.error(f"Job ID {msg.jid} not found for metadata update")
    writer.write(RESP_OK)
    await writer.drain()
    logger.info("Update processed successfully")

async def main():
    server = await asyncio.start_server(handle_client, "0.0.0.0", 9999)
    addr = server.sockets[0].getsockname()
    logger.info(f"Controller serving on {addr}")
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    try:
        import uvloop
        uvloop.install()
        logger.info("Using uvloop")
    except ImportError:
        logger.warning("uvloop not installed, using default event loop")

    if len(sys.argv) > 1:
        DESIRED_CONCURRENCY = int(sys.argv[1])
    asyncio.run(main())

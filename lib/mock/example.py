#!/usr/bin/env python

import asyncio
import os
import time
from asyncreflex import ReFlexHandler, JobCtx, FlowCtx
import logging

# Configuration
REQ_SIZE = 1024
CONCURRENCY = 1
DUMMY_DATA = os.urandom(REQ_SIZE)

# Logging Setup
logger = logging.getLogger("test")
logging.basicConfig(
    level=logging.DEBUG,  # Set to DEBUG level
    format="%(asctime)s - %(name)s - [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

async def io_task(i):
    logger.info(f"Starting IO task {i}")
    handler = ReFlexHandler("127.0.0.1:9999", "new_app", impl="O2")
    job_ctx = JobCtx(
        IOPS_SLO=1000,
        latency_us_SLO=500,
        rw_ratio=100,
        concurrency=10,
        capacity=1024,
    )
    await handler.allocate("Test", job_ctx)

    flow_ctx = FlowCtx(IOPS_SLO=10000, latency_us_SLO=500, rw_ratio=100)

    async with handler.register(flow=flow_ctx) as conn:
        await handler.is_connected()
        logger.info("Connection established and flow registered")

        timestamps = []
        got_response = asyncio.Event()

        def req_done_cb(buffer=None):
            elapsed_us = (time.time() - timestamps[-1]) * 1e6
            if buffer:
                logger.info(f"Read completed in {elapsed_us:.2f} µs")
                assert buffer == DUMMY_DATA, "Data mismatch"
                got_response.set()
            else:
                logger.info(f"Write completed in {elapsed_us:.2f} µs")
            logger.debug("req_done_cb executed")

        conn.set_req_done_cb(req_done_cb)

        # Write Data
        timestamps.append(time.time())
        conn.put_object(DUMMY_DATA, "1")

        # Read Data
        timestamps.append(time.time())
        conn.get_object("1")

        # Event loop to process events
        while not got_response.is_set():
            conn.poll()
        
        conn.remove_req_done_cb()

    await handler.free()
    logger.info(f"IO task {i} completed")

async def main():
    tasks = [io_task(i) for i in range(CONCURRENCY)]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    try:
        import uvloop
        uvloop.install()
    except ImportError:
        logger.warning("uvloop not installed")
    asyncio.run(main())
#!/usr/bin/env python

import asyncio
import unittest
import os
import time
import random
from asyncreflex import ReFlexHandler, JobCtx, FlowCtx
import logging

# Logging Setup
logger = logging.getLogger("ReFlexIOTests")
logging.basicConfig(
    level=logging.DEBUG,  # Set to DEBUG level
    format="%(asctime)s - %(name)s - [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

class ReFlexIOTests(unittest.IsolatedAsyncioTestCase):
    """Test cases for ReFlex IO operations."""

    async def asyncSetUp(self):
        # Setup that is run before each test method
        logger.info("Setting up test case")
        self.handler = ReFlexHandler("127.0.0.1:9999", "new_app", impl="O2")
        self.job_ctx = JobCtx(
            IOPS_SLO=1000,
            latency_us_SLO=500,
            rw_ratio=100,
            concurrency=10,
            capacity=1024 * 1024 * 8,  # Increased capacity for larger tests
        )
        await self.handler.allocate("TestJob", self.job_ctx)
        self.flow_ctx = FlowCtx(
            IOPS_SLO=10000,
            latency_us_SLO=500,
            rw_ratio=100,
        )

        # Manually enter the async context manager
        self.conn_cm = self.handler.register(flow=self.flow_ctx)
        self.conn = await self.conn_cm.__aenter__()
        await self.handler.is_connected()
        logger.info("Connection established and flow registered")

    async def asyncTearDown(self):
        # Teardown that is run after each test method
        logger.info("Tearing down test case")
        await self.conn_cm.__aexit__(None, None, None)
        await self.handler.free()

    async def test_write_read_aligned(self):
        """Test writing and reading data of sizes 1024 and 4096 bytes."""
        base_size = 512
        sizes = [base_size * (2 ** i) for i in range(13)]

        for size in sizes:
            data = os.urandom(size)
            obj_name = f"test_obj_{size}"
            await self.perform_read_after_write(data, obj_name)
            logger.info(f"Data of size {size} bytes written and read successfully")

    async def test_write_read_unaligned(self):
        """Test writing and reading data of varying sizes from 8 bytes to 1MB."""
        test_sizes = [random.randint(8, 512) for _ in range(3)]
        test_sizes += [random.randint(512 * (2**(i-1)), 512 * (2**i)) for i in range(1, 13)]
        for size in test_sizes:
            with self.subTest(size=size):
                data = os.urandom(size)
                obj_name = f"test_obj_{size}"
                await self.perform_read_after_write(data, obj_name)

    async def test_metadata_persistence(self):
        """Test if the controller memorizes the metadata across the connection (same job)."""
        data = os.urandom(512)
        obj_name = "persistent_obj"

        # Write data
        await self.perform_read_after_write(data, obj_name)

        # Read data again to verify metadata persistence
        await self.perform_read(data, obj_name)

    async def test_metadata_persistence_after_flush(self):
        pass

    async def perform_read_after_write(self, data, obj_name):
        timestamps = []
        write_complete = asyncio.Event()
        read_complete = asyncio.Event()

        def req_done_cb(buffer=None):
            elapsed_us = (time.time() - timestamps[-1]) * 1e6
            if buffer is not None:
                logger.info(f"Read of {len(buffer)} bytes completed in {elapsed_us:.2f} µs")
                self.assertEqual(buffer, data, f"Data mismatch for object {obj_name}")
                read_complete.set()
                logger.debug(f"req_done_cb for read executed")
            else:
                logger.info(f"Write completed")
                write_complete.set()
                logger.debug(f"req_done_cb for write executed")

        self.conn.set_req_done_cb(req_done_cb)

        # Write Data
        timestamps.append(time.time())
        self.conn.put_object(data, obj_name)

        # Wait for write to complete
        while not write_complete.is_set():
            self.conn.poll()
            await asyncio.sleep(0)

        # Proceed to read after write completes
        timestamps.append(time.time())
        read_buf = self.conn.get_object(obj_name)

        # Wait for read to complete
        while not read_complete.is_set():
            self.conn.poll()
            await asyncio.sleep(0)

        # Remove the callback after both write and read are complete
        self.conn.remove_req_done_cb()


    async def perform_read(self, data, obj_name):
        """Helper function to perform a read operation and verify data."""
        timestamps = []
        got_response = asyncio.Event()

        def req_done_cb(buffer=None):
            if buffer is not None:
                # Handle read completion
                got_response.set()
                logger.info(f"Read completed, data size: {len(buffer)}")
                # Process the data
            else:
                # Handle write completion
                logger.info("Write completed")

        self.conn.set_req_done_cb(req_done_cb)

        # Read Data
        timestamps.append(time.time())
        self.conn.get_object(obj_name)

        # Event loop to process events
        while not got_response.is_set():
            self.conn.poll()
            await asyncio.sleep(0)  # Yield control to the event loop
        

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    # Run the tests
    unittest.main()

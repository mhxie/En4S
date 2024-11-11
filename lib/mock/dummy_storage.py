#!/usr/bin/env python

import asyncio
import logging
import os
import sys
import ctypes
import time
import socket
import json
from datetime import datetime
from reflex import *
from asyncio import IncompleteReadError

# Configuration Constants
DEBUG_FLAG = True
USE_UVLOOP = True
LOG_INTERVAL = True
MAX_CONN_NUM = 8
STORAGE_MAXSIZE = 64 * 1024 * 1024  # Adjust as needed
CLIENT_IMPL = "PROTO"  # Options: "NONBLOCKING", "PROTO", "SOCKET"
BATCH_SIZE = 4

# Logging Setup
logger = logging.getLogger("main")
logging.basicConfig(
    level=logging.DEBUG if DEBUG_FLAG else logging.INFO,
    format="%(asctime)s - %(name)s - [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

# File Setup
today = datetime.today().strftime("%Y%m%d")
file_name = f"benchmark/data/{today}/logs_{CLIENT_IMPL}.txt"
os.makedirs(os.path.dirname(file_name), exist_ok=True)

# Global State (avoid if possible)
current_conn = 0

async def handle_client(reader, writer):
    """Handle incoming client connections."""
    global current_conn
    addr = writer.get_extra_info("peername")
    logger.info(f"Connected to {addr}")

    # Connection-specific variables
    buffer = bytearray()
    storage = bytearray(b"x" * STORAGE_MAXSIZE)
    header_size = ctypes.sizeof(ReFlexHeader)
    recv_pending = False
    header = None

    # Statistics
    stats = {
        "write_count": 0,
        "read_count": 0,
        "avg_recv": 0,
        "avg_sent": 0,
        "recv_count": 0,
        "recv_timestamps": [],
        "send_timestamps": [],
    }

    try:
        while True:
            # Read Data
            data = await reader.read(header_size * 16)
            print(f"Data size: {len(data)}")
            if not data:
                logger.info(f"Client {addr} disconnected")
                break

            # Update Stats
            stats["recv_timestamps"].append(time.time())
            buffer.extend(data)
            stats["avg_recv"] += len(data)
            stats["recv_count"] += 1

            # Process Messages
            while len(buffer) >= header_size:
                if not recv_pending:
                    header_bytes = buffer[:header_size]
                    buffer = buffer[header_size:]

                    # Deserialize Header
                    header = ReFlexHeader()
                    header.receiveSome(bytes(header_bytes))

                    # Handle Different Opcodes
                    if header.opcode == CMD_GET:
                        logger.debug(f"Received GET request from {addr}")
                        await handle_get(header, writer, storage, stats)
                    elif header.opcode == CMD_SET:
                        logger.debug(f"Received SET request from {addr}")
                        recv_pending = True
                        expected_size = header.lba_count * LBA_SIZE
                    elif header.opcode == CMD_REG:
                        await handle_register(header, writer)
                    else:
                        logger.warning(f"Unknown opcode {header.opcode}")
                else:
                    # Handle SET Data
                    logger.debug(f"Received SET data from {addr}: {len(buffer)}/{expected_size}")
                    if len(buffer) < expected_size:
                        # Wait for more data
                        break
                    data_bytes = buffer[:expected_size]
                    buffer = buffer[expected_size:]
                    await handle_set(header, data_bytes, writer, storage, stats)
                    logger.debug(f"SET request processed from {addr}")
                    recv_pending = False
    except Exception as e:
        logger.error(f"Error handling client {addr}: {e}")
    finally:
        # Cleanup
        writer.close()
        await writer.wait_closed()
        current_conn -= 1
        logger.info(f"Connection with {addr} closed")
        log_statistics(stats)

async def handle_get(header, writer, storage, stats):
    """Handle GET requests."""
    stats["read_count"] += 1
    start, end = calculate_storage_range(header, storage)
    response = storage[start:end]
    header.lba = int(time.time() * 1e6)
    header.lba_count = int.from_bytes(RESP_OK, "big")
    writer.write(bytes(header) + response)
    await writer.drain()
    stats["avg_sent"] += len(response) + ctypes.sizeof(ReFlexHeader)
    stats["send_timestamps"].append(time.time())

async def handle_set(header, data_bytes, writer, storage, stats):
    """Handle SET requests."""
    stats["write_count"] += 1
    start, end = calculate_storage_range(header, storage)
    storage[start:end] = data_bytes
    header.lba = int(time.time() * 1e6)
    header.lba_count = int.from_bytes(RESP_OK, "big")
    writer.write(bytes(header))
    await writer.drain()
    stats["avg_sent"] += ctypes.sizeof(ReFlexHeader)
    stats["send_timestamps"].append(time.time())

async def handle_register(header, writer):
    """Handle client registration."""
    global current_conn
    current_conn += 1
    if current_conn > MAX_CONN_NUM:
        current_conn -= 1
        header.lba_count = int.from_bytes(RESP_FAIL, "big")
        writer.write(bytes(header))
        await writer.drain()
        logger.info("Maximum connections reached, closing connection")
        writer.close()
    else:
        header.lba_count = int.from_bytes(RESP_OK, "big")
        writer.write(bytes(header))
        await writer.drain()
        logger.info(f"Client registered successfully, total connections: {current_conn}")

def calculate_storage_range(header, storage):
    """Calculate the start and end positions in the storage buffer."""
    start = (header.lba * LBA_SIZE) % STORAGE_MAXSIZE
    end = ((header.lba + header.lba_count) * LBA_SIZE) % STORAGE_MAXSIZE
    if start > end:
        return start, STORAGE_MAXSIZE
    return start, end

def log_statistics(stats):
    """Log connection statistics."""
    if stats["recv_count"] == 0:
        return
    stats["avg_recv"] /= stats["recv_count"]
    stats["avg_sent"] /= stats["recv_count"]
    logger.info(f"Average Received Bytes: {stats['avg_recv']}")
    logger.info(f"Average Sent Bytes: {stats['avg_sent']}")
    if LOG_INTERVAL:
        # Calculate intervals
        recv_intervals = [
            (t2 - t1) * 1e6 for t1, t2 in zip(stats["recv_timestamps"], stats["recv_timestamps"][1:])
        ]
        send_intervals = [
            (t2 - t1) * 1e6 for t1, t2 in zip(stats["send_timestamps"], stats["send_timestamps"][1:])
        ]
        if recv_intervals:
            avg_recv_interval = sum(recv_intervals) / len(recv_intervals)
            logger.info(f"Average Receive Interval: {avg_recv_interval:.2f} µs")
        if send_intervals:
            avg_send_interval = sum(send_intervals) / len(send_intervals)
            logger.info(f"Average Send Interval: {avg_send_interval:.2f} µs")
        # Log traces to file
        traces = {
            "recv_intervals": recv_intervals,
            "send_intervals": send_intervals,
        }
        with open(file_name, "a") as f:
            f.write(json.dumps(traces) + "\n")

async def main(port):
    """Main function to start the server."""
    server = await asyncio.start_server(handle_client, "0.0.0.0", port)
    addr = server.sockets[0].getsockname()
    logger.info(f"Serving on {addr}")

    # Remove existing log file
    if os.path.exists(file_name):
        os.remove(file_name)

    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    # Use uvloop if available and configured
    if USE_UVLOOP:
        try:
            import uvloop
            uvloop.install()
            logger.info("Using uvloop")
        except ImportError:
            logger.warning("uvloop not installed, using default event loop")

    # Parse command-line arguments for port
    port = 8888  # Default port
    if len(sys.argv) >= 2:
        port = int(sys.argv[1])

    # Run the server
    asyncio.run(main(port))
import time
import asyncio
from math import ceil
from typing import List, Tuple

import sys
import os

sys.path.append(os.getcwd())
import asynces


# Function to access chunks in storage
async def access_n_chunks(
    names: List[str],
    chunks: List[bytes] = None,
    handler=None,
    is_delete: bool = False,
    metric_prefix: str = "",
) -> Tuple[List, List[bytes]]:
    """
    Accesses multiple chunks (get, put, or delete) in storage.

    Args:
        names (List[str]): List of chunk names.
        chunks (List[bytes], optional): List of chunk data for writing. Defaults to None.
        handler: Storage handler.
        is_delete (bool, optional): Flag to indicate deletion. Defaults to False.

    Returns:
        Tuple[List, List[bytes]]: Metrics and list of chunks (for reads).
    """
    send_ts = []
    recv_ts = []
    metrics = []
    is_write = True

    if chunks is None:
        is_write = False
        chunks = [None] * len(names)
    else:
        assert len(names) == len(chunks)

    responses = []

    def accessCb(buf=None):
        nonlocal recv_ts, responses
        recv_ts.append(time.time())

        if buf:
            responses.append(buf)

    # 64MB / 128, 128 4KB chunks * 128 clients = 16K IOPS
    # 100GB / 128, 128 * 1600 4KB chunks
    if is_write:
        avg_len = sum(len(chunk) for chunk in chunks) / len(chunks)
        num_4k = ceil(avg_len / 4096)
    else:
        # FIXME: assume small objects
        num_4k = 1

    # 1MB = 512/(1024/4) = 2 IOPS
    flow_ctx = asynces.FlowCtx(
        IOPS_SLO=256 // num_4k,  # 256 * 128 = 64K 4KiB IOPS
        latency_us_SLO=2000,
        rw_ratio=0 if is_write else 100,
    )

    async with handler.register(flow_ctx) as conn:
        await handler.is_connected()

        conn.set_req_done_cb(accessCb)
        for i, name in enumerate(names):
            send_ts.append(time.time())
            if is_delete:
                conn.delete_object(name)
                continue
            if is_write:
                conn.put_object(chunks[i], name)
            else:
                conn.get_object(name)
        while len(recv_ts) < len(send_ts):
            fut = conn.poll()
            if fut:
                await fut
            else:
                await asyncio.sleep(0)

    for i in range(len(names)):
        if is_delete:
            metrics.append([metric_prefix + "delete", recv_ts[i] - send_ts[i], 0])
            continue
        if is_write:
            metrics.append(
                [metric_prefix + "set", recv_ts[i] - send_ts[i], len(chunks[i])]
            )
        else:
            metrics.append(
                [metric_prefix + "get", recv_ts[i] - send_ts[i], len(responses[i])]
            )
    return metrics, responses

import time
import asyncio
from math import ceil
from typing import List, Tuple, Dict, Any, Union
import boto3
import logging

import sys
import os

sys.path.append(os.getcwd())
import asynces

async def access_es_objects(
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
        # chunks = [None] * len(names)
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
                # print(f"Getting object {name}")
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

def access_s3_objects(
    bucket_name: str,
    object_keys: Union[str, List[str]],
    objects: Union[bytes, List[bytes]] = None,
) -> Tuple[List, Union[bytes, List[bytes]]]:
    """
    Accesses one or multiple objects in S3 storage using boto3.
    """
    session = boto3.Session()
    metrics = []
    t0 = time.time()
    
    s3_client = session.client('s3')
    
    # Convert single key/object to list for consistent processing
    if isinstance(object_keys, str):
        object_keys = [object_keys]
    if objects is not None and isinstance(objects, bytes):
        objects = [objects]
    
    if objects is None:
        # Read objects from S3
        try:
            data_list = []
            for key in object_keys:
                t_start = time.time()
                response = s3_client.get_object(Bucket=bucket_name, Key=key)
                data = response['Body'].read()
                t_end = time.time()
                metrics.append(["s3_get", t_end - t_start, len(data)])
                data_list.append(data)
                logging.info(f"Read {len(data)} bytes from S3 object {key}")
            
            t1 = time.time()
            metrics.append(["s3_get_total", t1 - t0, sum(len(d) for d in data_list)])
            
            return metrics, data_list if len(data_list) > 1 else data_list[0]
        except Exception as e:
            logging.error(f"Error reading from S3: {e}")
            raise
    else:
        # Write objects to S3
        try:
            for key, obj in zip(object_keys, objects):
                t_start = time.time()
                s3_client.put_object(Bucket=bucket_name, Key=key, Body=obj)
                t_end = time.time()
                metrics.append(["s3_put", t_end - t_start, len(obj)])
                logging.info(f"Wrote {len(obj)} bytes to S3 object {key}")
            
            t1 = time.time()
            metrics.append(["s3_put_total", t1 - t0, sum(len(obj) for obj in objects)])
            
            return metrics, None
        except Exception as e:
            logging.error(f"Error writing to S3: {e}")
            raise


# Function to invoke multiple subtasks
async def invoke_n_fanout_subtasks(
    args_list: List[Dict[str, Any]], handler, ex_region
) -> Tuple[List, List]:
    """
    Invokes multiple subtasks concurrently.

    Args:
        args_list (List[Dict[str, Any]]): List of arguments for each subtask.
        handler: Storage handler.

    Returns:
        Tuple[List, List]: Metrics and task returns.
    """
    subtasks = []
    for args in args_list:
        invoke_url = args.get("invoke_url") or os.getenv("next_url")
        app_url = args.get("app_url") or os.getenv("self_url")

        # Ensure we have either 'invoke_url' or 'app_url'
        if invoke_url:
            subtasks.append(
                handler.invoke(invoke_url, args, reserve_capacity=ex_region)
            )
        elif app_url:
            subtasks.append(handler.invoke(app_url, args, reserve_capacity=ex_region))
        else:
            raise ValueError(
                "Neither 'invoke_url' nor 'app_url' provided in args, and none available from environment"
            )

    function_rets = await asyncio.gather(*subtasks, return_exceptions=True)
    # print(f"Subtask returns: {function_rets}")
    metric_rets = []
    for ret in function_rets:
        # if isinstance(ret, Exception):
        #     logging.error(f"Subtask failed with exception: {ret}")
        # else:
        metric_rets += ret["metrics"]
    return metric_rets

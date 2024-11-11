import os
import asyncio
import time
import asynces
import copy
import logging
import numpy as np
from typing import List, Tuple, Dict, Any
from helper import access_es_objects, invoke_n_fanout_subtasks

# Configure logging
logging.basicConfig(level=logging.INFO)
LBA_SIZE = 512

# Application tasks
APP_TASKS = {
    "etl": {
        # "extract": None,   # Placeholder for terasort_mapper function
        # "transform": None,  # Placeholder for terasort_reducer function
        # "transform": None,  # Placeholder for terasort_reducer function
        # "load": None,  # Placeholder for terasort_reducer function
        "work": None,
    },
    # Add other applications if needed
}


async def actual_worker(args: Dict[str, Any], handler) -> Tuple[List, None]:
    metrics = []
    # perform write ephemeral storage
    names = [f"etl_{args['x']}_{args['y']}_{i}" for i in range(args["num_obj"])]
    new_metrics, _ = await access_es_objects(
        names,
        [os.urandom(4096)for _ in range(args["num_obj"])],
        handler,
        metric_prefix="etl_write",
    )
    metrics += new_metrics
    print(f"Worker {args['x']}_{args['y']} finished writing {args['num_obj']} objects")

    new_metrics, _ = await access_es_objects(
        names, None, handler, metric_prefix="etl_read"
    )
    metrics += new_metrics
    print(f"Worker {args['x']}_{args['y']} finished reading {args['num_obj']} objects")

    ret = {"metrics": metrics}
    if hasattr(handler, "get_invokee_state") and callable(handler.get_invokee_state):
        ret["invokee_ctx"] = handler.get_invokee_state()

    return ret


APP_TASKS["etl"]["work"] = actual_worker


# Function to invoke mapper tasks
async def invoke_n_workers(
    args: Dict[str, Any], handler, tot_size
) -> Tuple[List, Dict[str, Any]]:
    """
    Invokes mapper tasks.
    """
    args_list = []
    for i in range(args["composition"][0]):
        args_i = copy.deepcopy(args)
        args_i["x"] = i
        args_list.append(args_i)

    lba_per_worker = (
        2 * tot_size // args["composition"][0] // args["composition"][1] // LBA_SIZE
    )
    metrics = await invoke_n_fanout_subtasks(args_list, handler, lba_per_worker)

    return metrics, args


# Terasort large data pipeline (single iteration)
async def etl_pipeline(args: Dict[str, Any], handler):
    """
    ETL Pipeline
    """
    metrics = []

    for p in range(args["composition"][1]):
        args["stage"] = "work"
        args["y"] = p
        new_metrics, args = await invoke_n_workers(args, handler, args["size"])
        metrics += new_metrics
    await handler.free()

    return metrics, args


# Application pipelines
APP_PIPELINES = {
    "etl": etl_pipeline,
}


# Main Entry Point
async def multiplexor(event, context):
    """
    Main entry point for the serverless function.
    """
    try:
        app_name = os.getenv("app_name", "etl")
        stage = event.get("stage")
        app_name_with_idx = app_name + str(event.get("tid", 0))
        logging.info(f"Application: {app_name}, Stage: {stage}")
        handler = asynces.create_handler(
            event["es_type"],
            os.getenv(event["es_type"]),
            app_name_with_idx,
            ctx=event.get("invoker_ctx"),
            opt=event.get("opt"),
            impl=event.get("impl"),
        )

        if stage == "coordinator":
            num_lbas = 64 * 1024 * 1024 * 1024 * 3 // 512
            job_ctx = asynces.JobCtx(
                IOPS_SLO=100000,
                latency_us_SLO=1000,
                rw_ratio=50,
                capacity=num_lbas,
            )
            # this should only be called once
            await handler.allocate(
                name=app_name_with_idx, job=job_ctx  # subfolder name
            )
            pipeline = APP_PIPELINES.get(app_name)
            if not pipeline:
                raise ValueError(f"No pipeline registered for application '{app_name}'")
            t0 = time.time()
            metrics, _ = await pipeline(event, handler)
            t1 = time.time()
            metrics.append(["total", t1 - t0, 0])
            await handler.free()
            return metrics
        else:
            t0 = time.time()
            ret = await APP_TASKS[app_name][stage](event, handler)
            elapsed = time.time() - t0
            logging.info(f"Worker {stage} finished in {elapsed:.2f} seconds")
            return ret
    except Exception as e:
        logging.error(f"Error in multiplexor: {e}")
        raise


# Entry point for AWS Lambda or other serverless platforms
def lambda_handler(event, context):
    """
    AWS Lambda handler function.
    """
    return asyncio.run(multiplexor(event, context))

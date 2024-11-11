import os
import asyncio
import time
import asynces
import copy
import logging
import numpy as np
import mapper
import reducer
from access import access_n_chunks
from typing import List, Tuple, Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO)
LBA_SIZE = 512

# Application tasks
APP_TASKS = {
    "terasort": {
        "mapper": None,   # Placeholder for terasort_mapper function
        "reducer": None,  # Placeholder for terasort_reducer function
    },
    # Add other applications if needed
}

# Terasort Mapper Function
async def terasort_mapper(args: Dict[str, Any], handler) -> Tuple[List, None]:
    """
    Performs the mapper stage of the Terasort application.
    """
    try:
        iteration = args.get("iteration", 0)
        return await mapper.map(
            "terasort",
            handler,
            args["max"],
            args["composition"][1],
            args["id"],
            iteration  # Pass iteration
        )
    except Exception as e:
        logging.error(f"Error in terasort_mapper: {e}")
        raise

# Terasort Reducer Function
async def terasort_reducer(args: Dict[str, Any], handler) -> Tuple[List, None]:
    """
    Performs the reducer stage of the Terasort application.
    """
    try:
        iteration = args.get("iteration", 0)
        return await reducer.reduce(
            "terasort",
            handler,
            args["id"],
            args["max"],
            args["composition"][0],
            args["composition"][1],
            iteration  # Pass iteration
        )
    except Exception as e:
        logging.error(f"Error in terasort_reducer: {e}")
        raise

# Register tasks
APP_TASKS["terasort"]["mapper"] = terasort_mapper
APP_TASKS["terasort"]["reducer"] = terasort_reducer

# Function to invoke multiple subtasks
async def invoke_n_fanout_subtasks(args_list: List[Dict[str, Any]], handler, ex_region) -> Tuple[List, List]:
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
            subtasks.append(handler.invoke(invoke_url, args, reserve_capacity = ex_region))
        elif app_url:
            subtasks.append(handler.invoke(app_url, args, reserve_capacity = ex_region))
        else:
            raise ValueError("Neither 'invoke_url' nor 'app_url' provided in args, and none available from environment")
    
    function_rets = await asyncio.gather(*subtasks, return_exceptions=True)
    metric_rets = []
    for ret in function_rets:
        # if isinstance(ret, Exception):
        #     logging.error(f"Subtask failed with exception: {ret}")
        # else:
        logging.info(f"metric ret: {ret['metrics']}")
        metric_rets += ret['metrics']
    return metric_rets

# Function to generate data chunks for mappers
async def gen_m_chunks(args: Dict[str, Any], handler) -> Tuple[List, Dict[str, Any]]:
    """
    Generates data chunks for mappers for the current iteration.
    In real world, can be any frontend data ingestor.
    """
    metrics = []
    M = args["composition"][0]
    tot_size = int(args["size"])  # Total size in bytes
    element_size = 8  # Size of int64 in bytes
    tot_count = tot_size // element_size  # Total number of elements
    per_count = tot_count // M  # Number of elements per mapper
    iteration = args.get("iteration", 0)  # Default to 0 if not provided

    name_list = []
    chunk_list = []
    for chunk_id in range(M):
        t1 = time.time()
        data = np.random.randint(0, args["max"], (per_count,), dtype="int64")
        data_bytes = data.tobytes()
        name_list.append(f"unsorted_array_{iteration}_{chunk_id}")
        chunk_list.append(data_bytes)
        t2 = time.time()
        metrics.append(["gen", t2 - t1, len(data_bytes)])

    new_metrics, _ = await access_n_chunks(name_list, chunk_list, handler)
    metrics += new_metrics

    args["stage"] = "mapper"
    return metrics, args

# Function to invoke mapper tasks
async def invoke_m_mapper(args: Dict[str, Any], handler, tot_size) -> Tuple[List, Dict[str, Any]]:
    """
    Invokes mapper tasks.
    """
    assert args["stage"] == "mapper"
    args_list = []
    for i in range(args["composition"][0]):
        args_i = copy.deepcopy(args)
        args_i["id"] = i
        args_list.append(args_i)
    
    lba_per_mapper = 2 * tot_size // args["composition"][0] // LBA_SIZE
    metrics = await invoke_n_fanout_subtasks(args_list, handler, lba_per_mapper)
    args["stage"] = "reducer"
    return metrics, args

# Function to invoke reducer tasks
async def invoke_n_reducer(args: Dict[str, Any], handler, tot_size) -> Tuple[List, Dict[str, Any]]:
    """
    Invokes reducer tasks.
    """
    assert args["stage"] == "reducer"
    args_list = []
    for i in range(args["composition"][1]):
        args_i = copy.deepcopy(args)
        args_i["id"] = i
        args_list.append(args_i)
    lba_per_reducer = 2 * tot_size // args["composition"][1] // LBA_SIZE
    metrics = await invoke_n_fanout_subtasks(args_list, handler, lba_per_reducer)
    args["stage"] = "end"
    return metrics, args

# Terasort pipeline that processes data over multiple iterations
async def terasort_pipeline(args: Dict[str, Any], handler):
    """
    Terasort pipeline that processes data over multiple iterations.
    """
    metrics = []
    iterations = args.get('iterations', 1)
    for iteration in range(iterations):
        logging.info(f"Starting iteration {iteration + 1}/{iterations}")
        args['iteration'] = iteration
        # Generate data chunks for this iteration
        new_metrics, args = await gen_m_chunks(args, handler)
        metrics += new_metrics
        # Invoke mapper tasks for this iteration
        new_metrics, args = await invoke_m_mapper(args, handler, args["size"])
        metrics += new_metrics
        # Invoke reducer tasks for this iteration
        new_metrics, args = await invoke_n_reducer(args, handler, args["size"])
        metrics += new_metrics

    return metrics

# Terasort large data pipeline (single iteration)
async def terasort_large_pipeline(args: Dict[str, Any], handler):
    """
    Terasort pipeline for processing large data in a single iteration.
    """
    metrics = []
    # Generate data chunks
    new_metrics, args = await gen_m_chunks(args, handler)
    metrics += new_metrics
    # Invoke mapper tasks
    new_metrics, args = await invoke_m_mapper(args, handler, args["size"])
    metrics += new_metrics
    # Invoke reducer tasks
    new_metrics, args = await invoke_n_reducer(args, handler, args["size"])
    metrics += new_metrics
    await handler.free()
    return metrics

# Application pipelines
APP_PIPELINES = {
    "terasort_pipelined": terasort_pipeline,
    "terasort_large": terasort_large_pipeline,
    # Add other pipelines if needed
}

# Main multiplexor function
async def multiplexor(event, context):
    """
    Main entry point for the serverless function.
    """
    try:
        app_name = os.getenv("app_name")
        if not app_name:
            raise ValueError("Environment variable 'app_name' is not set.")
        stage = event["stage"]
        logging.info(f"Entering stage: {stage}")
        handler = asynces.create_handler(
            event["es_type"],
            os.getenv(event["es_type"]),
            '-'.join([app_name, str(event["tid"])]),
            ctx=event.get("invoker_ctx"),
            opt=event.get("opt"),
            impl=event.get("impl"),
        )
        # Determine the pipeline type
        pipeline_type = event.get('pipeline_type', 'pipelined')  # Default to 'pipelined'
        pipeline_key = f"{app_name}_{pipeline_type}"
        pipeline = APP_PIPELINES.get(pipeline_key)
        if not pipeline:
            raise ValueError(f"Invalid pipeline type: {pipeline_type}")
        if stage == "coordinator":
            size = event.get("size", 0)
            num_lbas = 8 * size // LBA_SIZE # /512*4
            job_ctx = asynces.JobCtx(
                IOPS_SLO=100000,
                latency_us_SLO=1000,
                rw_ratio=50,
                capacity=num_lbas,
            )
            await handler.allocate(
                name = '-'.join([app_name, str(event["tid"])]),
                job = job_ctx
            )
            pipeline_args = event
            t0 = time.time()
            if callable(pipeline):
                metrics = await pipeline(pipeline_args, handler)
            else:
                metrics = []
                for func in pipeline:
                    logging.info(f"Coordinator handling pipeline {func.__name__}")
                    new_metrics = await func(pipeline_args, handler)
                    metrics += new_metrics
            t1 = time.time()
            metrics.append(["total", t1 - t0, 0])
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

    Args:
        event: Event data passed by AWS Lambda.
        context: Runtime information provided by AWS Lambda.

    Returns:
        Result from the multiplexor function.
    """
    return asyncio.run(multiplexor(event, context))
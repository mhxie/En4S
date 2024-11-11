import os
import asyncio
import time
import logging
import copy
from typing import List, Dict, Any, Tuple
from helper import access_es_objects, access_s3_objects, invoke_n_fanout_subtasks

# Import necessary modules (ensure these are accessible)
try:
    import cv2_helper  # Your custom module for OpenCV operations
except ImportError:
    print(f"This is an encode-only stage")
try:
    import iio_helper  # Your custom module for image I/O operations
except ImportError:
    print(f"This is a detect-only stage")
import asynces  # Your custom module for asynchronous storage handling

# Configure logging
logging.basicConfig(level=logging.INFO)

# Application tasks registry
APP_TASKS = {}

# Application pipelines registry
APP_PIPELINES = {}

LBA_SIZE = 512


# Split Stage Function
async def split_stage(args: Dict[str, Any], handler) -> Tuple[List, Any]:
    """
    Splits the input video into frames and stores them as chunks.
    """
    assert args["stage"] == "coordinator"
    input_bucket = args["input_bucket"]
    input_video_key = args["input_video_key"]
    num_frames = args.get("num_frames", 100)  # Default to 100 frames
    metrics = []

    # Load video from S3 using access_s3_object
    metrics_load, video_data = access_s3_objects(input_bucket, input_video_key)
    metrics += metrics_load

    # Split video into frames
    t_split_start = time.time()
    frames = iio_helper.split(video_data, max_frames=num_frames)
    t_split_end = time.time()
    split_time = t_split_end - t_split_start
    metrics.append(["split_time", split_time, len(video_data)])

    args["actual_num_frames"] = len(frames)
    num_frames = min(num_frames, args["actual_num_frames"])
    logging.info(f"Actual number of frames: {args['actual_num_frames']}")
    # Store frames as chunks using access_es_objects
    frame_names = [
        f"{input_video_key}_frame{idx}" for idx in range(num_frames)
    ]
    metrics_store, _ = await access_es_objects(
        frame_names, frames, handler, metric_prefix="split_"
    )
    metrics += metrics_store

    args["video_size"] = len(video_data)
    args["stage"] = "detector"

    return metrics, args


# Detect Stage Function
async def detect_stage(args: Dict[str, Any], handler) -> Tuple[List, Any]:
    """
    Performs object detection on frames.
    """
    assert args["stage"] == "detector"
    input_video_key = args["input_video_key"]
    frames_of_interest = args.get("frames_of_interest", None)
    frame_names = [f"{input_video_key}_frame{idx}" for idx in frames_of_interest]
    detection_result_names = [
        f"{input_video_key}_frame{idx}_detect" for idx in frames_of_interest
    ]
    metrics = []

    # Retrieve frames using access_es_objects
    metrics_get, frames_data = await access_es_objects(
        frame_names, None, handler, metric_prefix="detect0_"
    )
    metrics += metrics_get

    # Perform detection on each frame
    detection_result_data = []
    for frame_data in frames_data:
        t_detect_start = time.time()
        img = cv2_helper.decode(frame_data)
        outputs = cv2_helper.detect(img)
        outputs_serialized = cv2_helper.json_serialize(outputs)
        # Prepare for storage
        detection_result_data.append(bytes(outputs_serialized, "utf-8"))
        t_detect_end = time.time()
        metrics.append(["detect_frame", t_detect_end - t_detect_start, len(frame_data)])

    # Store detection results using access_es_objects
    new_metrics, _ = await access_es_objects(
        detection_result_names, detection_result_data, handler, metric_prefix="detect1_"
    )
    metrics += new_metrics
    ret = {"metrics": metrics}
    if hasattr(handler, "get_invokee_state") and callable(handler.get_invokee_state):
        ret["invokee_ctx"] = handler.get_invokee_state()

    return ret


# Track Stage Function
async def track_stage(args: Dict[str, Any], handler) -> Tuple[List, Any]:
    """
    Annotates frames based on detection results.
    """
    assert args["stage"] == "tracker"
    input_video_key = args["input_video_key"]
    frames_of_interest = args.get("frames_of_interest", None)
    frame_names = [f"{input_video_key}_frame{idx}" for idx in frames_of_interest]

    processed_frame_names = [
        f"{input_video_key}_frame{idx}_processed" for idx in frames_of_interest
    ]
    detection_result_names = [
        f"{input_video_key}_frame{idx}_detect" for idx in frames_of_interest
    ]
    metrics = []

    # Retrieve frames using access_es_objects
    new_metrics, frames_data = await access_es_objects(
        frame_names, None, handler, metric_prefix="tracker0_"
    )
    metrics += new_metrics

    # Retrieve detection results using access_es_objects
    metrics_get_results, detection_results_data = await access_es_objects(
        detection_result_names, None, handler, metric_prefix="tracker1_"
    )
    metrics += metrics_get_results

    # Annotate each frame
    processed_frames = []
    for frame_data, detection_data_bytes in zip(frames_data, detection_results_data):
        t_annotate_start = time.time()
        img = cv2_helper.decode(frame_data)
        detection_outputs = cv2_helper.json_deserialize(
            detection_data_bytes.decode("utf-8")
        )
        annotated_img = cv2_helper.annotate(img, detection_outputs)
        encoded_frame = cv2_helper.encode(annotated_img)
        processed_frames.append(encoded_frame)
        t_annotate_end = time.time()
        metrics.append(["annotate", t_annotate_end - t_annotate_start, len(frame_data)])

    # Store processed frames using access_es_objects
    # new_metrics, _ = await access_es_objects(
    #     processed_frame_names, processed_frames, handler, metric_prefix="tracker_"
    # )
    new_metrics, _ = access_s3_objects(
        args["output_bucket"], processed_frame_names, processed_frames,
    )
    metrics += new_metrics
    ret = {"metrics": metrics}
    if hasattr(handler, "get_invokee_state") and callable(handler.get_invokee_state):
        ret["invokee_ctx"] = handler.get_invokee_state()

    return ret


# Merge Stage Function
async def merge_stage(args: Dict[str, Any], handler) -> Tuple[List, Any]:
    """
    Merges processed frames into a video and stores it.

    Args:
        args: Arguments dictionary.
        handler: Storage handler.

    Returns:
        Metrics and updated args.
    """
    assert args["stage"] == "merger"
    num_frames = min(args.get("num_frames", 100), args["actual_num_frames"])
    input_video_key = args["input_video_key"]
    print(f"Actual number of frames: {args['actual_num_frames']}")
    processed_frame_names = [
        f"{input_video_key}_frame{idx}_processed" for idx in range(num_frames)
    ]
    output_bucket = args["output_bucket"]
    output_video_key = args["output_video_key"]
    metrics = []

    # Retrieve processed frames using access_es_objects
    # new_metrics, processed_frames_data = await access_es_objects(
    #     processed_frame_names, None, handler, metric_prefix="merge_"
    # )
    new_metrics, processed_frames_data = access_s3_objects(
        output_bucket, processed_frame_names
    )
    metrics += new_metrics

    # Merge frames into video
    t_merge_start = time.time()
    output_video_path = f"/tmp/output.mp4"
    iio_helper.merge(processed_frames_data, output_video_path)
    t_merge_end = time.time()
    total_frames_size = sum(len(f) for f in processed_frames_data)
    metrics.append(["merge", t_merge_end - t_merge_start, total_frames_size])

    # Read merged video data
    with open(output_video_path, "rb") as f:
        output_video_data = f.read()

    # Store output video to S3 using access_s3_object
    new_metrics, _ = access_s3_objects(
        output_bucket, output_video_key, output_video_data
    )
    metrics += new_metrics

    args["stage"] = "end"
    return metrics, args


async def invoke_m_detector(
    args: Dict[str, Any], handler, tot_size
) -> Tuple[List, Dict[str, Any]]:
    args_list = []
    num_frames = min(args.get("num_frames", 100), args["actual_num_frames"])
    num_detectors = args["composition"][0]
    assert (
        num_frames >= num_detectors
    ), "Number of frames should be greater than number of detectors"

    frame_splits = list(range(num_frames))
    for i in range(num_detectors):
        args_i = copy.deepcopy(args)
        args_i["id"] = i
        args_i["frames_of_interest"] = frame_splits[i::num_detectors]
        args_list.append(args_i)

    lba_per_detector = 16 * tot_size // num_detectors // LBA_SIZE
    metrics = await invoke_n_fanout_subtasks(args_list, handler, lba_per_detector)

    args["stage"] = "tracker"
    return metrics, args


async def invoke_n_tracker(
    args: Dict[str, Any], handler, tot_size
) -> Tuple[List, Dict[str, Any]]:
    args_list = []
    num_frames = min(args.get("num_frames", 100), args["actual_num_frames"])
    num_trackers = args["composition"][1]
    assert (
        num_frames >= num_trackers
    ), "Number of frames should be greater than number of trackers"

    frame_splits = list(range(num_frames))
    for i in range(num_trackers):
        args_i = copy.deepcopy(args)
        args_i["id"] = i
        args_i["frames_of_interest"] = frame_splits[i::num_trackers]
        args_list.append(args_i)

    lba_per_tracker = 8 * tot_size // num_trackers // LBA_SIZE
    metrics = await invoke_n_fanout_subtasks(args_list, handler, lba_per_tracker)
    args["stage"] = "merger"
    return metrics, args


# Register Analytics Tasks
APP_TASKS["analytics"] = {
    "splitter": split_stage,
    "detector": detect_stage,  # fanout stage not returning args
    "tracker": track_stage,  # fanout stage not returning args
    "merger": merge_stage,
}


# Analytics Pipeline
async def analytics_pipeline(args: Dict[str, Any], handler):
    """
    Orchestrates the analytics pipeline stages.
    """
    metrics = []
    # Split stage
    new_metrics, args = await split_stage(args, handler)
    metrics += new_metrics
    logging.info(f"Split stage completed")
    # Detect stage
    new_metrics, args = await invoke_m_detector(args, handler, args["video_size"])
    metrics += new_metrics
    logging.info(f"Detect stage completed")
    # Track stage
    new_metrics, args = await invoke_n_tracker(args, handler, args["video_size"])
    metrics += new_metrics
    logging.info(f"Track stage completed")
    # Merge stage
    new_metrics, args = await merge_stage(args, handler)
    metrics += new_metrics
    logging.info(f"Merge stage completed")
    return metrics, args


# Register Analytics Pipeline
APP_PIPELINES["analytics"] = analytics_pipeline


# Main Entry Point
async def multiplexor(event, context):
    """
    Main entry point for the serverless function.
    """
    try:
        app_name = os.getenv("app_name", "analytics")
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
            # 16MB max video, 64MB frames * 3 stage
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


# Lambda Handler
def lambda_handler(event, context):
    """
    AWS Lambda handler function.
    """
    return asyncio.run(multiplexor(event, context))

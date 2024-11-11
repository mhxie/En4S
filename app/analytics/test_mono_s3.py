import asyncio
import time
import cv2_helper
import iio_helper
import numpy as np
import logging
import os
from typing import List, Dict, Any
import argparse
import aioboto3

import sys
import os
sys.path.append(os.getcwd())
from asynces import AsyncS3Handler, JobCtx


# Configure logging
logging.basicConfig(level=logging.INFO)

S3_BUCKET_NAME = 'sls-video-src'
INPUT_VIDEO_KEY = '../workloads/pexels_traffic_video.mp4'  # S3 key for the input video
OUTPUT_VIDEO_KEY = '../workloads/output_s3.mp4'  # S3 key for the processed output video

class AsyncVideoProcessor:
    def __init__(self, handler, mode='batch'):
        self.handler = handler
        self.mode = mode
        self.metrics = []
        self.processed_frames = []
        self.video_data = None

    async def read_video_from_s3(self):
        """
        Reads the video from S3 using aioboto3 and stores it in self.video_data.
        """
        logging.info("Reading video from S3 using aioboto3...")
        self.video_data = None  # Reset video data

        session = aioboto3.Session()
        async with session.client('s3') as s3_client:
            try:
                response = await s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=INPUT_VIDEO_KEY)
                self.video_data = await response['Body'].read()
                logging.info(f"Read {len(self.video_data)} bytes from S3.")
            except Exception as e:
                logging.error(f"Error reading video from S3: {e}")
                raise

    async def write_video_to_s3(self, video_path):
        """
        Writes the processed video to S3 using aioboto3.
        """
        logging.info("Writing processed video to S3 using aioboto3...")
        with open(video_path, 'rb') as f:
            processed_video_data = f.read()

        session = aioboto3.Session()
        async with session.client('s3') as s3_client:
            try:
                await s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=OUTPUT_VIDEO_KEY, Body=processed_video_data)
                logging.info(f"Wrote processed video to S3: {OUTPUT_VIDEO_KEY}")
            except Exception as e:
                logging.error(f"Error writing video to S3: {e}")
                raise

    async def process_batch(self):
        """
        Processes the video in batch mode.
        Uses asynces for storing temporary data.
        """
        # Stage 0: Split video into frames
        t0 = time.time()
        logging.info("Splitting video into frames...")
        frames = iio_helper.split(self.video_data)
        t1 = time.time()
        split_time = t1 - t0
        self.metrics.append(('split_time', split_time))
        logging.info(f"Split video into {len(frames)} frames in {split_time:.2f} seconds.")

        # Stage 1 & 2: Detect and annotate objects in each frame
        detect_times = []
        annotate_times = []
        for i, frame_data in enumerate(frames):
            logging.info(f"Processing frame {i+1}/{len(frames)}...")

            # Store frame data to temporary storage using asynces
            frame_key = f"frame_{i}"
            frame_processed_key = f"processed_frame_{i}"

            # Store frame_data to asynces with a callback
            frame_put_done = asyncio.Event()
            def put_frame_callback():
                frame_put_done.set()

            async with self.handler.register() as conn:
                conn.add_req_done_cb(put_frame_callback)
                conn.put_object(frame_data, frame_key)
                while not frame_put_done.is_set():
                    await conn.poll()
                    await asyncio.sleep(0.01)

            # Retrieve frame_data from asynces
            frame_get_done = asyncio.Event()
            retrieved_frame_data = None
            def get_frame_callback(data):
                nonlocal retrieved_frame_data
                retrieved_frame_data = data
                frame_get_done.set()

            async with self.handler.register() as conn:
                conn.add_req_done_cb(get_frame_callback)
                conn.get_object(frame_key)
                while not frame_get_done.is_set():
                    await conn.poll()
                    await asyncio.sleep(0.01)

            # Now process the frame
            img = cv2_helper.decode(retrieved_frame_data)

            # Detect objects
            t_detect_start = time.time()
            outputs = cv2_helper.detect(img)
            t_detect_end = time.time()
            detect_times.append(t_detect_end - t_detect_start)

            # Annotate frame
            t_annotate_start = time.time()
            annotated_img = cv2_helper.annotate(img, outputs)
            t_annotate_end = time.time()
            annotate_times.append(t_annotate_end - t_annotate_start)

            # Encode annotated frame
            encoded_frame = cv2_helper.encode(annotated_img)

            # Store processed frame to asynces
            processed_put_done = asyncio.Event()
            def put_processed_callback():
                processed_put_done.set()

            async with self.handler.register() as conn:
                conn.add_req_done_cb(put_processed_callback)
                conn.put_object(encoded_frame, frame_processed_key)
                while not processed_put_done.is_set():
                    await conn.poll()
                    await asyncio.sleep(0.01)

            # Retrieve processed frame from asynces
            processed_get_done = asyncio.Event()
            retrieved_processed_frame = None
            def get_processed_callback(data):
                nonlocal retrieved_processed_frame
                retrieved_processed_frame = data
                processed_get_done.set()

            async with self.handler.register() as conn:
                conn.add_req_done_cb(get_processed_callback)
                conn.get_object(frame_processed_key)
                while not processed_get_done.is_set():
                    await conn.poll()
                    await asyncio.sleep(0.01)

            # Append to processed_frames
            self.processed_frames.append(retrieved_processed_frame)

        # Record detection and annotation times
        avg_detect_time = sum(detect_times) / len(detect_times) if detect_times else 0
        avg_annotate_time = sum(annotate_times) / len(annotate_times) if annotate_times else 0
        self.metrics.append(('average_detect_time', avg_detect_time))
        self.metrics.append(('average_annotate_time', avg_annotate_time))
        logging.info(f"Average detection time per frame: {avg_detect_time:.2f} seconds.")
        logging.info(f"Average annotation time per frame: {avg_annotate_time:.2f} seconds.")

        # Stage 3: Merge annotated frames back into video
        t_merge_start = time.time()
        output_video_path = "/tmp/processed_video.mp4"
        iio_helper.merge(self.processed_frames, output_video_path)
        t_merge_end = time.time()
        merge_time = t_merge_end - t_merge_start
        self.metrics.append(('merge_time', merge_time))
        logging.info(f"Merged frames into video in {merge_time:.2f} seconds.")

        return output_video_path

    async def process_stream(self):
        """
        Processes the video in stream mode.
        Uses asynces for storing temporary data.
        """
        t_total_start = time.time()
        # Prepare video writer
        output_video_path = "/tmp/processed_video.mp4"
        merger = iio_helper.get_merger(output_video_path)

        frame_generator = iio_helper.split_iter(self.video_data)
        frame_index = 0

        detect_times = []
        annotate_times = []
        for frame_data in frame_generator:
            frame_index += 1
            logging.info(f"Processing frame {frame_index}...")

            # Store frame data to temporary storage using asynces
            frame_key = f"frame_{frame_index}"
            frame_processed_key = f"processed_frame_{frame_index}"

            # Store frame_data to asynces with a callback
            frame_put_done = asyncio.Event()
            def put_frame_callback():
                frame_put_done.set()

            async with self.handler.register() as conn:
                conn.add_req_done_cb(put_frame_callback)
                conn.put_object(frame_data, frame_key)
                while not frame_put_done.is_set():
                    await conn.poll()
                    await asyncio.sleep(0.01)

            # Retrieve frame_data from asynces
            frame_get_done = asyncio.Event()
            retrieved_frame_data = None
            def get_frame_callback(data):
                nonlocal retrieved_frame_data
                retrieved_frame_data = data
                frame_get_done.set()

            async with self.handler.register() as conn:
                conn.add_req_done_cb(get_frame_callback)
                conn.get_object(frame_key)
                while not frame_get_done.is_set():
                    await conn.poll()
                    await asyncio.sleep(0.01)

            # Now process the frame
            img = cv2_helper.decode(retrieved_frame_data)

            # Detect objects
            t_detect_start = time.time()
            outputs = cv2_helper.detect(img)
            t_detect_end = time.time()
            detect_times.append(t_detect_end - t_detect_start)

            # Annotate frame
            t_annotate_start = time.time()
            annotated_img = cv2_helper.annotate(img, outputs)
            t_annotate_end = time.time()
            annotate_times.append(t_annotate_end - t_annotate_start)

            # Encode annotated frame
            encoded_frame = cv2_helper.encode(annotated_img)

            # Store processed frame to asynces
            processed_put_done = asyncio.Event()
            def put_processed_callback():
                processed_put_done.set()

            async with self.handler.register() as conn:
                conn.add_req_done_cb(put_processed_callback)
                conn.put_object(encoded_frame, frame_processed_key)
                while not processed_put_done.is_set():
                    await conn.poll()
                    await asyncio.sleep(0.01)

            # Retrieve processed frame from asynces
            processed_get_done = asyncio.Event()
            retrieved_processed_frame = None
            def get_processed_callback(data):
                nonlocal retrieved_processed_frame
                retrieved_processed_frame = data
                processed_get_done.set()

            async with self.handler.register() as conn:
                conn.add_req_done_cb(get_processed_callback)
                conn.get_object(frame_processed_key)
                while not processed_get_done.is_set():
                    await conn.poll()
                    await asyncio.sleep(0.01)

            # Write frame to video
            iio_helper.merge_one(merger, retrieved_processed_frame)

        # Finalize video
        iio_helper.close_merger(merger)
        t_total_end = time.time()
        total_time = t_total_end - t_total_start
        self.metrics.append(('total_processing_time', total_time))
        avg_detect_time = sum(detect_times) / len(detect_times) if detect_times else 0
        avg_annotate_time = sum(annotate_times) / len(annotate_times) if annotate_times else 0
        self.metrics.append(('average_detect_time', avg_detect_time))
        self.metrics.append(('average_annotate_time', avg_annotate_time))
        logging.info(f"Processed video in {total_time:.2f} seconds.")
        logging.info(f"Average detection time per frame: {avg_detect_time:.2f} seconds.")
        logging.info(f"Average annotation time per frame: {avg_annotate_time:.2f} seconds.")

        return output_video_path

    async def run_test(self):
        await self.read_video_from_s3()

        if self.mode == 'batch':
            output_video_path = await self.process_batch()
        elif self.mode == 'stream':
            output_video_path = await self.process_stream()
        else:
            raise ValueError("Invalid mode. Choose 'batch' or 'stream'.")

        await self.write_video_to_s3(output_video_path)

        # Output metrics
        logging.info("Processing metrics:")
        for metric_name, value in self.metrics:
            logging.info(f"{metric_name}: {value:.2f} seconds")

# Main execution
async def main():
    parser = argparse.ArgumentParser(description='Async Video Processor Test')
    parser.add_argument('--mode', choices=['batch', 'stream'], default='batch',
                        help='Processing mode: batch or stream')
    args = parser.parse_args()

    # Initialize the AsyncS3Handler (for temporary data storage)
    handler = AsyncS3Handler(bucket_name=S3_BUCKET_NAME)
    await handler.allocate(name='test', job=JobCtx())

    # Create the video processor
    processor = AsyncVideoProcessor(handler, mode=args.mode)

    # Run the test
    await processor.run_test()

    # Clean up
    await handler.free()

if __name__ == '__main__':
    asyncio.run(main())
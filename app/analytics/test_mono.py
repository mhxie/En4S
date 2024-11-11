import cv2 as cv
import numpy as np
import time
import cv2_helper
import iio_helper
import sys
import os
import argparse

def original_mode(video_input_path, video_output_path, frame_count):
    """
    Processes video in the original mode: reads frames, processes them, and writes to output video.
    """
    latency = {0: [], 1: [], 2: [], 3: []}
    input_cap = cv.VideoCapture(video_input_path)
    num_frames = int(input_cap.get(cv.CAP_PROP_FRAME_COUNT))

    frame_width = int(input_cap.get(cv.CAP_PROP_FRAME_WIDTH))
    frame_height = int(input_cap.get(cv.CAP_PROP_FRAME_HEIGHT))
    size = (frame_width, frame_height)

    fourcc = cv.VideoWriter_fourcc(*'mp4v')
    result = cv.VideoWriter(video_output_path, fourcc, 30, size)

    count = 0
    while input_cap.isOpened():
        print(f"Processing frame {count}")
        t0 = time.time() * 1000
        ret, frame = input_cap.read()
        if not ret:
            break
        t1 = time.time() * 1000
        print(f"Local capture time = {t1 - t0:.2f} ms")
        outputs = cv2_helper.detect(frame)
        t2 = time.time() * 1000
        print(f"Detection time = {t2 - t1:.2f} ms")
        res = cv2_helper.annotate(frame, outputs)
        t3 = time.time() * 1000
        print(f"Annotation time = {t3 - t2:.2f} ms")
        result.write(res)
        t4 = time.time() * 1000
        print(f"Local writeback time = {t4 - t3:.2f} ms")
        latency[0].append(t1 - t0)
        latency[1].append(t2 - t1)
        latency[2].append(t3 - t2)
        latency[3].append(t4 - t3)
        count += 1
        if count >= frame_count:
            break
        if cv.waitKey(10) & 0xFF == ord('q'):
            break

    input_cap.release()
    result.release()

    file_size = os.path.getsize(video_input_path)
    bytes_per_chunk = file_size / num_frames * 100
    ms_per_chunk = bytes_per_chunk / 4096 * 2  # Assuming 2ms per 4K chunk
    print(f"Average IO latency per 100 frames = {ms_per_chunk:.2f} ms")
    avg_latency = [sum(latency[i])/len(latency[i]) for i in range(4)]
    print(f"Average processing latency per frame:")
    print(f"  Capture: {avg_latency[0]:.2f} ms")
    print(f"  Detection: {avg_latency[1]:.2f} ms")
    print(f"  Annotation: {avg_latency[2]:.2f} ms")
    print(f"  Writeback: {avg_latency[3]:.2f} ms")

def batch_mode(video_input_path, video_output_path, frame_count):
    """
    Processes video in batch mode: splits video into frames, processes them, and merges back.
    """
    metrics = []
    processed_frames = []
    ts = time.time()
    t0 = time.time()
    print("Splitting started")
    frames = iio_helper.split(video_input_path, max_frames=frame_count)
    print("Splitting ended")
    t1 = time.time()
    split_time = (t1 - t0)
    metrics.append(split_time / len(frames) if frames else 0)

    detect_times = []
    annotate_times = []
    for i, frame_data in enumerate(frames):
        print(f"Processing frame {i}")
        t2 = time.time()
        img = cv2_helper.decode(frame_data)
        outputs = cv2_helper.detect(img)
        t3 = time.time()
        detect_times.append(t3 - t2)
        res = cv2_helper.annotate(img, outputs)
        processed_frames.append(cv2_helper.encode(res))
        t4 = time.time()
        annotate_times.append(t4 - t3)

    t5 = time.time()
    iio_helper.merge(processed_frames, video_output_path)
    t6 = time.time()
    merge_time = (t6 - t5)
    metrics.append(merge_time / len(frames) if frames else 0)
    te = time.time()
    total_time = te - ts

    # Print metrics
    avg_detect_time = sum(detect_times) / len(detect_times) if detect_times else 0
    avg_annotate_time = sum(annotate_times) / len(annotate_times) if annotate_times else 0
    print(f"Stage 0 (Split): {split_time*1000:.2f} ms total, {metrics[0]*1000:.2f} ms per frame")
    print(f"Stage 1 (Detect): {avg_detect_time*1000:.2f} ms per frame")
    print(f"Stage 2 (Annotate): {avg_annotate_time*1000:.2f} ms per frame")
    print(f"Stage 3 (Merge): {merge_time*1000:.2f} ms total, {metrics[1]*1000:.2f} ms per frame")
    print(f"Total elapsed time: {total_time:.2f} s")

def stream_mode(video_input_path, video_output_path, frame_count):
    """
    Processes video in stream mode: processes frames one by one and writes directly to output video.
    """
    metrics = {'split': [], 'detect': [], 'annotate': [], 'merge': []}
    ts = time.time()
    t0 = time.time()

    merger = iio_helper.get_merger(video_output_path)
    frame_iter = iio_helper.split_iter(video_input_path, max_frames=frame_count)
    for i, frame_data in enumerate(frame_iter):
        t1 = time.time()
        metrics['split'].append(t1 - t0)
        print(f"Stream processing frame {i}")
        img = cv2_helper.decode(frame_data)
        t_detect_start = time.time()
        outputs = cv2_helper.detect(img)
        t_detect_end = time.time()
        metrics['detect'].append(t_detect_end - t_detect_start)
        t_annotate_start = time.time()
        res = cv2_helper.annotate(img, outputs)
        t_annotate_end = time.time()
        metrics['annotate'].append(t_annotate_end - t_annotate_start)
        processed_frame = cv2_helper.encode(res)
        t_merge_start = time.time()
        iio_helper.merge_one(merger, processed_frame)
        t_merge_end = time.time()
        metrics['merge'].append(t_merge_end - t_merge_start)
        t0 = time.time()
    iio_helper.close_merger(merger)
    te = time.time()
    total_time = te - ts

    # Compute average times
    avg_split_time = sum(metrics['split']) / len(metrics['split']) if metrics['split'] else 0
    avg_detect_time = sum(metrics['detect']) / len(metrics['detect']) if metrics['detect'] else 0
    avg_annotate_time = sum(metrics['annotate']) / len(metrics['annotate']) if metrics['annotate'] else 0
    avg_merge_time = sum(metrics['merge']) / len(metrics['merge']) if metrics['merge'] else 0

    print(f"Stage 0 (Split): {avg_split_time*1000:.2f} ms per frame")
    print(f"Stage 1 (Detect): {avg_detect_time*1000:.2f} ms per frame")
    print(f"Stage 2 (Annotate): {avg_annotate_time*1000:.2f} ms per frame")
    print(f"Stage 3 (Merge): {avg_merge_time*1000:.2f} ms per frame")
    print(f"Total elapsed time: {total_time:.2f} s")

def main():
    parser = argparse.ArgumentParser(description='Video Analytics Test')
    parser.add_argument('--mode', choices=['original', 'batch', 'stream'], default='batch',
                        help='Demo mode to run: original, batch, or stream')
    parser.add_argument('--input', type=str, default='../workloads/pexels_traffic_video.mp4',
                        help='Path to input video file')
    parser.add_argument('--output', type=str, default='../workloads/output.mp4',
                        help='Path to output video file')
    parser.add_argument('--max-frames', type=int, default=1000,)
    args = parser.parse_args()

    if args.mode == 'original':
        original_mode(args.input, args.output, args.max_frames)
    elif args.mode == 'batch':
        batch_mode(args.input, args.output, args.max_frames)
    elif args.mode == 'stream':
        stream_mode(args.input, args.output, args.max_frames)
    else:
        print(f"Unknown mode {args.mode}. Use --help for usage information.")

if __name__ == '__main__':
    main()
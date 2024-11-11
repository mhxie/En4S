import imageio.v3 as iio
import imageio
import io


def split(video, max_frames=128):
    frames = []
    frame_counter = 0
    # supports video reading in bytes or file path
    # if isinstance(video, str):
    for frame in iio.imiter(video, plugin="FFMPEG", extension=".mp4"):
        # print(f"reading frame-{frame_counter}")
        # print(f"frame: {frame.nbytes} bytes")
        frame_counter += 1
        if frame_counter > max_frames:
            break
        jpeg_encode = iio.imwrite("<bytes>", frame, extension=".jpeg")
        # jpeg_encode = iio.imwrite(
        #    "<bytes>", frame, plugin="opencv", extension=".jpg"
        # )
        frames.append(jpeg_encode)
        # split video by frame
    #    else:
    #        raw_frames = iio.imread(video, plugin="FFMPEG", extension=".mp4")
    #        for frame in raw_frames:
    #            frame_counter += 1
    #            jpeg_encode = iio.imwrite(
    #                "<bytes>", frame, plugin="opencv", extension=".jpg"
    #            )
    #            frames.append(jpeg_encode)
    return frames


def decode(img):
    return iio.imread(img, extension=".jpg")


def encode(img):
    return iio.imwrite("<bytes>", img, extension=".jpg")


def split_iter(video, max_frames=128, batch=1):
    """Used if the memory cannot hold all frames"""
    frame_counter = 0
    frame_batch = []
    batch_counter = 0
    for frame in iio.imiter(video, plugin="FFMPEG", extension=".mp4"):
        frame_counter += 1
        if frame_counter > max_frames:
            break
        if batch == 1:
            jpeg_encode = iio.imwrite("<bytes>", frame, extension=".jpeg")
            yield jpeg_encode
        elif batch > 1:
            jpeg_encode = iio.imwrite("<bytes>", frame, extension=".jpeg")
            frame_batch.append(jpeg_encode)
            batch_counter += 1
            if batch_counter >= batch:
                batch_counter = 0
                frame_batch = []
                yield frame_batch
        else:
            assert False, "Please set a reasonable batch"
    print(f"Finished the split of {frame_counter} frames")


# def merge(frames, dst=None, fps=30):
#     if not dst:
#         dst = io.BytesIO()
#     writer = imageio.get_writer(dst, fps=fps)

#     for frame in frames:
#         # img = iio.imread(frame, plugin="opencv", extension=".jpg")
#         # writer.append_data(img)
#         writer.append_data(iio.imread(frame, extension=".jpg"))
#     writer.close()

#     return dst

def merge(frames, output_path, fps=30):
    writer = imageio.get_writer(output_path, fps=fps, codec='libx264', quality=7)

    for frame in frames:
        # Read each frame from bytes
        img = imageio.imread(io.BytesIO(frame))
        writer.append_data(img)

    writer.close()


def get_merger(dst, fps=30):
    return imageio.get_writer(dst, fps=fps)


def merge_one(writer, frame):
    writer.append_data(iio.imread(frame, extension=".jpg"))


def close_merger(writer):
    writer.close()


def merge_tobytes(frames, fps=30):
    pass

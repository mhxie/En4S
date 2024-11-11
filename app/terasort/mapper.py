import numpy as np
import time
from access import access_n_chunks


def sort_chunk(chunk_byte, MAX, num_reducer):
    array = np.frombuffer(chunk_byte, dtype="int64").copy()
    array.sort()
    # shuffle_size = MAX // num_reducer
    shuffles = []
    boundaries = np.linspace(0, MAX, num_reducer + 1)
    for k in range(num_reducer):
        shuffle_min = boundaries[k]
        shuffle_max = boundaries[k + 1]
        if k == num_reducer - 1:
            mask = (array >= shuffle_min) & (array <= shuffle_max)
        else:
            mask = (array >= shuffle_min) & (array < shuffle_max)
        shuffle = array[mask]
        shuffles.append(shuffle)
    return shuffles


func_list = {
    "terasort": sort_chunk,
}


async def map(func, handler, MAX, num_reducer, j, iteration):
    metrics = []

    get_metrics, responses = await access_n_chunks(
        [f"unsorted_array_{iteration}_{j}"], None, handler, metric_prefix="mapper_"
    )
    metrics += get_metrics
    print(f"Got unsorted_array_{iteration}_{j}, length={len(responses[0])}")

    sort_start_t = time.time()
    # Sort and shuffle data
    shuffles = func_list[func](responses[0], MAX, num_reducer)
    metrics += [["mapper_sort", time.time() - sort_start_t, len(responses[0])]]

    # Write sorted shuffles to storage
    shuffle_names = [f"sorted_shuffle_{iteration}_{k}_{j}" for k in range(num_reducer)]
    shuffle_bytes = [shuffle.tobytes() for shuffle in shuffles]
    set_metrics, _ = await access_n_chunks(
        shuffle_names, shuffle_bytes, handler, metric_prefix="mapper_"
    )
    metrics += set_metrics

    print(f"Mapper {j}: All put operations completed")
    ret = {"metrics": metrics}
    if hasattr(handler, "get_invokee_state") and callable(handler.get_invokee_state):
        ret["invokee_ctx"] = handler.get_invokee_state()

    return ret

import numpy as np
import time
from access import access_n_chunks


def sort_shuffles(shuffle_bytes, reducer_id, MAX, num_reducer):
    arrays = [
        np.frombuffer(shuffle_byte, dtype="int64") for shuffle_byte in shuffle_bytes
    ]
    array = np.concatenate(arrays)
    array.sort()
    return array


func_list = {
    "terasort": sort_shuffles,
}


async def reduce(func, handler, reducer_id, MAX, num_mapper, num_reducer, iteration):
    metrics = []

    # Get shuffles from storage
    shuffle_names = [
        f"sorted_shuffle_{iteration}_{reducer_id}_{j}" for j in range(num_mapper)
    ]
    get_metrics, responses = await access_n_chunks(
        shuffle_names, None, handler, metric_prefix="reducer_"
    )
    metrics += get_metrics

    # Merge and sort shuffles
    sort_start_t = time.time()
    array = func_list[func](responses, reducer_id, MAX, num_reducer)
    array_bytes = array.tobytes()
    metrics += [["reducer_sort", time.time() - sort_start_t, len(array_bytes)]]

    # Write sorted array to storage
    set_metrics, _ = await access_n_chunks(
        [f"sorted_array_{iteration}_{reducer_id}"],
        [array_bytes],
        handler,
        metric_prefix="reducer_",
    )
    metrics += set_metrics

    ret = {"metrics": metrics}
    if hasattr(handler, "get_invokee_state") and callable(handler.get_invokee_state):
        ret["invokee_ctx"] = handler.get_invokee_state()

    return ret

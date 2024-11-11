import numpy as np
import os
import tempfile
import concurrent.futures
import json

# Configuration: list of tuples (Data Size in MB, Mapper Count, Reducer Count)
CONFIGURATIONS = [
    (16, 4, 4),  # Example: 16MB data size, 4 mappers, 4 reducers
    # Add more configurations as needed
    (64, 8, 8),  # Larger configuration example
]

MAX_INT = 10000000  # Maximum integer value for random data generation

def map_local(mapper_id, N, iteration, temp_dir):
    shuffle_sizes = []
    # Load unsorted chunk
    file_path = os.path.join(temp_dir, f"unsorted_array_{iteration}_{mapper_id}.npy")
    data = np.load(file_path)
    # Sort data
    sorted_data = np.sort(data)
    # Determine partition boundaries
    boundaries = np.linspace(0, MAX_INT, N + 1)
    # Partition and save shuffle files
    for reducer_id in range(N):
        lower_bound = boundaries[reducer_id]
        upper_bound = boundaries[reducer_id + 1]
        # Select data within the range
        if reducer_id == N - 1:
            # Include the upper bound in the last partition
            partition_data = sorted_data[(sorted_data >= lower_bound) & (sorted_data <= upper_bound)]
        else:
            partition_data = sorted_data[(sorted_data >= lower_bound) & (sorted_data < upper_bound)]
        shuffle_file = os.path.join(
            temp_dir, f"sorted_shuffle_{iteration}_{reducer_id}_{mapper_id}.npy"
        )
        np.save(shuffle_file, partition_data)
        shuffle_size = os.path.getsize(shuffle_file)
        shuffle_sizes.append(shuffle_size)
    # Return shuffle sizes (one per reducer)
    return mapper_id, shuffle_sizes

def reduce_local(reducer_id, M, iteration, temp_dir):
    shuffle_sizes = []
    # Load all shuffle files for this reducer
    data_list = []
    for mapper_id in range(M):
        shuffle_file = os.path.join(
            temp_dir, f"sorted_shuffle_{iteration}_{reducer_id}_{mapper_id}.npy"
        )
        data = np.load(shuffle_file)
        data_list.append(data)
        shuffle_size = os.path.getsize(shuffle_file)
        shuffle_sizes.append(shuffle_size)
    # Merge and sort
    merged_data = np.concatenate(data_list)
    sorted_data = np.sort(merged_data)
    # Save the sorted array
    sorted_file = os.path.join(
        temp_dir, f"sorted_array_{iteration}_{reducer_id}.npy"
    )
    np.save(sorted_file, sorted_data)
    merged_size = os.path.getsize(sorted_file)
    # Return shuffle sizes (one per mapper) and merged size
    return reducer_id, shuffle_sizes, merged_size

def main():
    num_iterations = 4  # Number of iterations for each configuration
    all_results = []  # Collect results for all configurations
    for data_size_mb, M, N in CONFIGURATIONS:
        print(f"\nRunning Terasort with Data Size: {data_size_mb}MB, Mappers: {M}, Reducers: {N}")
        config_results = {
            'data_size_mb': data_size_mb,
            'mappers': M,
            'reducers': N,
            'iterations': []
        }
        for iteration in range(num_iterations):
            print(f"\nStarting iteration {iteration + 1}/{num_iterations}")
            # Create temporary directory for this iteration
            temp_dir = tempfile.mkdtemp()
            print(f"Temporary directory created at {temp_dir}")
    
            # Generate random data
            data_size = (data_size_mb * 1024 * 1024) // 8  # Number of int64 integers
            data = np.random.randint(0, MAX_INT, data_size, dtype="int64")
            print(f"Generated {data.nbytes / (1024 * 1024):.2f} MB of data")
            sorted_data_full = np.sort(data)  # For verification
    
            # Split data among mappers and save to local files
            chunk_size = len(data) // M
            for mapper_id in range(M):
                start_idx = mapper_id * chunk_size
                end_idx = (mapper_id + 1) * chunk_size if mapper_id != M - 1 else len(data)
                chunk = data[start_idx:end_idx]
                file_path = os.path.join(temp_dir, f"unsorted_array_{iteration}_{mapper_id}.npy")
                np.save(file_path, chunk)
    
            # Initialize metrics dictionary
            metrics = {
                'mapper_shuffles': {},   # Key: mapper_id, Value: list of shuffle sizes (one per reducer)
                'reducer_shuffles': {},  # Key: reducer_id, Value: list of shuffle sizes (one per mapper)
                'merged_sizes': {},      # Key: reducer_id, Value: size in bytes
                'mapper_times': {},      # Placeholder for timing data (empty lists)
                'reducer_times': {},     # Placeholder for timing data (empty lists)
            }
    
            # Run mappers in parallel
            with concurrent.futures.ProcessPoolExecutor() as executor:
                futures = [executor.submit(map_local, mapper_id, N, iteration, temp_dir)
                           for mapper_id in range(M)]
                for future in concurrent.futures.as_completed(futures):
                    mapper_id, shuffle_sizes = future.result()
                    metrics['mapper_shuffles'][str(mapper_id)] = shuffle_sizes
                    metrics['mapper_times'][str(mapper_id)] = []  # Empty list for timing data
            print(f"Iteration {iteration}: All mappers completed")
    
            # Run reducers in parallel
            with concurrent.futures.ProcessPoolExecutor() as executor:
                futures = [executor.submit(reduce_local, reducer_id, M, iteration, temp_dir)
                           for reducer_id in range(N)]
                for future in concurrent.futures.as_completed(futures):
                    reducer_id, shuffle_sizes, merged_size = future.result()
                    metrics['reducer_shuffles'][str(reducer_id)] = shuffle_sizes
                    metrics['merged_sizes'][str(reducer_id)] = merged_size
                    metrics['reducer_times'][str(reducer_id)] = []  # Empty list for timing data
            print(f"Iteration {iteration}: All reducers completed")
    
            # Verify results
            sorted_arrays = []
            for reducer_id in range(N):
                file_path = os.path.join(
                    temp_dir, f"sorted_array_{iteration}_{reducer_id}.npy"
                )
                data_part = np.load(file_path)
                sorted_arrays.append(data_part)
            final_sorted_data = np.concatenate(sorted_arrays)
            # Since we partitioned by ranges, and ranges do not overlap,
            # the concatenated array should be sorted if reducers' outputs are sorted.
            # So we can avoid sorting the concatenated array.
            assert np.array_equal(sorted_data_full, final_sorted_data), "Sorted arrays do not match."
            print(f"Iteration {iteration}: Verification successful")
    
            # Append metrics to config_results
            config_results['iterations'].append(metrics)
    
            # Clean up temporary files
            for root, dirs, files in os.walk(temp_dir, topdown=False):
                for name in files:
                    os.remove(os.path.join(root, name))
            os.rmdir(temp_dir)
            print(f"Temporary directory {temp_dir} deleted")
    
        # Save metrics for this configuration to a file
        output_filename = f"terasort_shuffles_{data_size_mb}MB_{M}M_{N}R.json"
        with open(output_filename, 'w') as f:
            json.dump(config_results, f, indent=2)
        print(f"Shuffle sizes for configuration saved to {output_filename}")
        all_results.append(config_results)
    
    # Optionally, save all results to a single file
    with open("terasort_all_shuffles.json", 'w') as f:
        json.dump(all_results, f, indent=2)
    print("All shuffle sizes saved to terasort_all_shuffles.json")

if __name__ == "__main__":
    main()
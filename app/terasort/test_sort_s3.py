import unittest
import numpy as np
import asyncio
import mapper
import reducer

import sys
import os
sys.path.append(os.getcwd())
import asynces

# MN_Pairs = [(128, 128)]
# MN_Pairs = [(16, 16)]
MN_Pairs = [(4, 4)]
MAX_INT = 10000000
NUM_ITER = 4
DATA_SIZE = (1 * 1024 * 1024) // 8  # Number of int64 integers in 1MB

class TestTerasortMethods(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestTerasortMethods, self).__init__(*args, **kwargs)

        async def create_and_allocate():
            self.handler = asynces.create_handler("s3", "es-store-0")
            await self.handler.allocate("test", job_ctx=asynces.JobCtx())

        asyncio.run(create_and_allocate())

    def __gen_data(self):
        self.data = np.random.randint(0, MAX_INT, DATA_SIZE, dtype="int64")
        print(f"Generated {self.data.nbytes / 1024 / 1024:.2f} MB of data")
        self.sorted_data = np.sort(self.data)

    async def __prep_data(self, M, iteration):
        async with self.handler.register() as conn:
            recv = 0

            def dummy_cb(buf=None):
                nonlocal recv
                recv += 1

            conn.add_req_done_cb(dummy_cb)
            chunk_size = len(self.data) // M
            for j in range(M):
                chunk = self.data[j * chunk_size : (j + 1) * chunk_size]
                conn.put_object(chunk.tobytes(), f"unsorted_array_{iteration}_{j}")
            expected_recv = M
            while recv < expected_recv:
                await conn.poll()

    async def __sort_mxn(self, M, N, iteration):
        # Run mappers in parallel
        mapper_tasks = [
            mapper.map("terasort", self.handler, MAX_INT, N, j, iteration) for j in range(M)
        ]
        await asyncio.gather(*mapper_tasks)
        print(f"Iteration {iteration}: All mappers completed")
        # Run reducers in parallel
        reducer_tasks = [
            reducer.reduce("terasort", self.handler, k, MAX_INT, M, N, iteration) for k in range(N)
        ]
        await asyncio.gather(*reducer_tasks)
        print(f"Iteration {iteration}: All reducers completed")

    async def __verify_results(self, N, iteration):
        async with self.handler.register() as conn:
            recv = 0
            responses = []

            def dummy_cb(buf=None):
                nonlocal recv, responses
                recv += 1
                responses.append(buf)

            conn.add_req_done_cb(dummy_cb)
            for k in range(N):
                conn.get_object(f"sorted_array_{iteration}_{k}")
            expected_recv = N
            while recv < expected_recv:
                await conn.poll()
            sorted_arrays = responses
            array = np.concatenate(
                [np.frombuffer(data, dtype="int64") for data in sorted_arrays]
            )
            # Verify the sorted array
            assert np.array_equal(self.sorted_data, array), "Sorted arrays do not match."

    async def __cleanup(self, M, N, iteration):
        async with self.handler.register() as conn:
            recv = 0

            def dummy_cb(buf=None):
                nonlocal recv
                recv += 1

            conn.add_req_done_cb(dummy_cb)
            total_deletes = 0
            # Delete unsorted arrays
            for j in range(M):
                conn.delete_object(f"unsorted_array_{iteration}_{j}")
                total_deletes += 1
                # Delete sorted shuffles
                for k in range(N):
                    conn.delete_object(f"sorted_shuffle_{iteration}_{k}_{j}")
                    total_deletes += 1
            # Delete sorted arrays
            for k in range(N):
                conn.delete_object(f"sorted_array_{iteration}_{k}")
                total_deletes += 1
            while recv < total_deletes:
                await conn.poll()

    def test_sort(self):
        num_iterations = NUM_ITER
        M, N = MN_Pairs[0]
        for iteration in range(num_iterations):
            print(f"Starting iteration {iteration + 1}/{num_iterations}")
            self.__gen_data()
            asyncio.run(self.__prep_data(M, iteration))
            asyncio.run(self.__sort_mxn(M, N, iteration))
            asyncio.run(self.__verify_results(N, iteration))
            asyncio.run(self.__cleanup(M, N, iteration))

if __name__ == "__main__":
    unittest.main()
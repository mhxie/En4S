# cython: language_level=3
# cython: profile=False
# flow.pyx
#
# Flow Control implementation

import asyncio
import time
import collections
# from math import log2, ceil

include "consts.pxi"


class FlowEnforcer():
    def __init__(self, block_size=4):
        self.transmission_d = (block_size / LAMBDA_BW) * 1e6
        self.recv_dq = collections.deque([])
        self.recv_ts_dq = collections.deque([])
        self.send_ts_dq = collections.deque([])
        self.server_dq = collections.deque([])
        self.start = None
        self.batch_size = 0
        self.sent = 0
        self.rtt_diff = 0
        self.HAI_count = 0
        self.last_send = time.time_ns()
        self.windowed_bytes = 0
        self.suggested_win = MAX_RECV_PENDING
        self.CURR_MAX = 64

    def set_flow_target(self, exp_iops, exp_us, rw_ratio=100):
        # expect_max_factor = max(ceil(log2(expected_rate / LAMBDA_CL_IOPS)), 0)
        # self.CURR_MAX = min(2**expect_max_factor, SEND_MAX)
        self.bw = exp_iops * rw_ratio * (4096+24) / 100 + exp_iops * (100-rw_ratio) * 24 / 100
        self.rate = exp_iops
        self.exp_rate = self.rate
        self.interval = 1e9 / exp_iops
        # print(f"exp_iops={exp_iops}, self.bw = {self.bw}")
        self.last_rtt = 1e6 # 1000000ns
        self.max_rtt = exp_us * 1000
        self.rtt_high = self.max_rtt
        self.min_rtt = int(exp_us * 1000 * 0.5)
        self.rtt_low = self.min_rtt

    def recv_one(self):
        # now = time.time_ns()
        # self.recv_dq.append(recv)
        # self.recv_ts_dq.append(now)
        # self.windowed_bytes += recv
        rtt = time.time_ns() - self.send_ts_dq.popleft()
        self.timely_adjust_rate(rtt)
        # if len(self.recv_dq) > QUEUE_SIZE:
        #     recv = self.recv_dq.popleft()
        #     self.windowed_bytes -= recv
        #     ts = self.recv_ts_dq.popleft()
        #     self.bw = self.windowed_bytes / (now - ts) * 1e9

    def send_now(self):
        now = time.time_ns()
        elapsed = now - self.last_send
        if elapsed < self.interval:
            return False
        else:
            self.last_send = now
            self.send_ts_dq.append(now)
            return True

    def flow_check(self, qlen, server_d = 0):
        """Periodically check to notify server for possible changes"""

        # Elevate flow priority
        # Demote flow priority
        return False

    def get_next_batch(self, outstanding_num):
        if not self.start:
            self.start = self.next_req_time = time.time_ns()
            # self.calibrate_time = self.start + CALIBRATE_INTERVAL
        if ADAPTIVE_LEVEL == 0:
            # bound the number of outstanding requests
            if outstanding_num < self.CURR_MAX:
                offset = time.time_ns() - self.next_req_time
                if offset > 0:
                    self.batch_size = int(offset // self.interval) + 1
                    self.batch_size = min(
                        self.batch_size, self.CURR_MAX - outstanding_num)
                    self.sent += self.batch_size
                    self.next_req_time = self.start + self.sent*self.interval
                    return self.batch_size
                # if now > self.calibrate_time:
                #     self.calibrate_time += CALIBRATE_INTERVAL
                #     self.next_req_time = self.start + self.sent*self.interval
        elif ADAPTIVE_LEVEL == 1:
            pass
            # if outstanding_num <= self.CURR_MAX:
            #     offset = time.time_ns() - self.next_req_time
            #     if offset > 0:
            #         self.batch_size = int(offset // self.interval) + 1
            #         self.sent += self.batch_size
            #         self.next_req_time = self.start + self.sent*self.interval
            #         return self.batch_size
            # elif outstanding_num > 1.5 * self.CURR_MAX:
            #     self.CURR_MAX = 2 * self.CURR_MAX
        else:
            pass
        return 0

    def timely_adjust_rate(self, new_rtt):
        new_rtt_diff = new_rtt - self.last_rtt
        self.last_rtt = new_rtt
        self.rtt_diff = (1-EWMA_ALPHA) * self.rtt_diff + EWMA_ALPHA * new_rtt_diff
        normalized_gradient = self.rtt_diff / self.min_rtt
        if new_rtt < self.rtt_low:
            # self.increase_rate(rtt_diff)
            # print(f"{new_rtt} is lower than lower limit, increasing rate")
            self.rate = self.rate + self.exp_rate * 0.02
            if new_rtt < self.min_rtt:
                self.min_rtt = new_rtt
        elif new_rtt > self.rtt_high:
            # self.decrease_rate(rtt_diff)
            # print(f"{new_rtt} exceeds upper limit, decresing rate")
            self.rate = self.rate * (1 - EWMA_BETA * (1 - self.max_rtt / new_rtt))
            if new_rtt > self.max_rtt:
                self.max_rtt = new_rtt
        else:
            if normalized_gradient <= 0:
                self.HAI_count += 1
                if self.HAI_count < 5:
                    self.rate = self.rate + self.exp_rate * 0.02
                else:
                    # print(f"HAI hits: increasing rate faster")
                    self.rate = self.rate + self.exp_rate * 0.1
            else:
                self.HAI_count = 0
                # print(f"HAI stops: decreasing rate")
                if normalized_gradient > 1:
                    # print(f"gradient is {normalized_gradient}")
                    normalized_gradient = 1
                self.rate = self.rate * (1 - EWMA_BETA * normalized_gradient)
        self.interval = 1e9 / self.rate

    def update_service_t(self, server_d):
        self.server_dq.append(server_d)
        if len(self.server_dq) >= 16:
            self.server_dq.popleft()
        last_recv = 0
        last_send = 0
        rtt = last_recv - last_send - server_d - self.transmission_d
        if rtt < self.min_rtt:
            self.min_rtt = rtt
        return rtt

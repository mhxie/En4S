# messages.py

import ctypes

# Message Structures
class ContextMsg(ctypes.Structure):
    _pack_ = 1
    _fields_ = [
        ("ip", ctypes.c_ubyte * 4),
        ("port", ctypes.c_uint),
        ("jid", ctypes.c_uint),
        ("capacity", ctypes.c_uint),
        ("app_name", ctypes.c_ulong),
        ("next_lba", ctypes.c_ulong),
        ("first_lba", ctypes.c_ulong),
        ("last_lba", ctypes.c_ulong),
        ("metadata_size", ctypes.c_ulong),
    ]

class AllocateMsg(ctypes.Structure):
    _pack_ = 1
    _fields_ = [
        ("app_name", ctypes.c_ulong),  # Fixed-length character array
        ("IOPS_SLO", ctypes.c_ulong),
        ("latency_us_SLO", ctypes.c_ulong),
        ("encoded_DAG_len", ctypes.c_ulong),
        ("req_size", ctypes.c_uint),
        ("rw_ratio", ctypes.c_uint),
        ("concurrency", ctypes.c_uint),
        ("capacity", ctypes.c_uint),
        ("num_regions", ctypes.c_uint),
    ]

class DeallocateMsg(ctypes.Structure):
    _pack_ = 1
    _fields_ = [
        ("jid", ctypes.c_ulong),
    ]

class RegisterMsg(ctypes.Structure):
    _pack_ = 1
    _fields_ = [
        ("jid", ctypes.c_ulong),
        ("IOPS_SLO", ctypes.c_ulong),
        ("latency_us_SLO", ctypes.c_ulong),
        ("req_size", ctypes.c_uint),
        ("rw_ratio", ctypes.c_uint),
        ("sequential", ctypes.c_bool),
        ("persistent", ctypes.c_bool),
    ]

class DeregisterMsg(ctypes.Structure):
    _pack_ = 1
    _fields_ = [
        ("jid", ctypes.c_ulong),
        ("fid", ctypes.c_ulong),
    ]

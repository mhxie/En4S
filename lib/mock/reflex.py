"""ReFlex header class"""
import ctypes

LBA_SIZE = 512
# ReFlex cmd code
CMD_GET = 0x00
CMD_SET = 0x01
CMD_SET_NO_ACK = 0x02
CMD_REG = 0x03

# Controller op code
CTRL_REG = b"\x00"
CTRL_DEREG = b"\x01"
CTRL_UPDATE = b"\x02"
CTRL_ALLOC = b"\x03"
CTRL_DEALLOC = b"\x04"

RESP_OK = b"\x00"
RESP_FAIL = b"\x01"
RESP_EINVAL = b"\x04"


class ReFlexHeader(ctypes.Structure):
    """ReFlex header class that can be parsed by both languages"""

    _pack_ = 1
    _fields_ = [
        ("magic", ctypes.c_short),
        ("opcode", ctypes.c_short),
        # ('req_handle', ctypes.c_void_p),
        ("req_handle", ctypes.py_object),
        ("lba", ctypes.c_long),
        ("lba_count", ctypes.c_int),
    ]

    def send(self):
        """translate this ctype to byte stream"""
        return bytes(self)[:]

    def receiveSome(self, some_bytes) -> None:
        """translate byte stream to this ctype"""
        fit = min(len(some_bytes), ctypes.sizeof(self))
        ctypes.memmove(ctypes.addressof(self), some_bytes, fit)

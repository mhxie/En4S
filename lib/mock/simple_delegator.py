""" Dummy delegator for testing purposes.
"""

import socket
import ctypes
from reflex import ReFlexHeader, CMD_GET, CMD_SET


def send_cmd(host, port, opcode, lba, lba_count, obj=None) -> bytes:
    """Send a command to the server and return the response."""
    ret = bytearray()

    header = ReFlexHeader()
    header.magic = ctypes.sizeof(ReFlexHeader)
    header.opcode = opcode
    header.lba = lba
    header.lba_count = lba_count
    header.req_handle = id(ret)
    header_bytes = bytes(header)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    sock.connect((host, port))
    sock.send(header_bytes)
    if opcode == CMD_SET:
        sock.send(obj)

    _ = sock.recv(24)

    if opcode == CMD_GET:
        ret.extend(sock.recv(lba_count * 4096))

    sock.close()

    return ret


def handler(event, _) -> bytes:
    """Forward the request to the server, and return the response back to its invoker"""
    return send_cmd(
        host=event["ip"],
        port=event["port"],
        opcode=event["opcode"],
        lba=event["lba"],
        lba_count=event["lba_count"],
        obj=event["obj"],
    )

import asyncio
import os, ctypes
from reflex import *


dummy_data = os.urandom(LBA_SIZE * 8)


async def reflex_client(opcode, count):
    reader, writer = await asyncio.open_connection("127.0.0.1", 8888)

    header = ReFlexHeader()
    header.magic = ctypes.sizeof(ReFlexHeader)
    header.opcode = opcode
    header.lba_count = count
    header_bytes = bytes(header)

    print(f"Send: {header_bytes!r}")

    writer.write(header_bytes)
    if opcode == CMD_GET:
        data = await reader.readexactly(LBA_SIZE * count)
        print(f"Received: {data!r}")
    elif opcode == CMD_SET:
        writer.write(dummy_data[: LBA_SIZE * count])
        await writer.drain()

    writer.write_eof()
    print("Close the connection")
    writer.close()
    await writer.wait_closed()


asyncio.run(reflex_client(CMD_GET, 4))
asyncio.run(reflex_client(CMD_SET, 4))

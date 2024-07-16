import asyncio
from contextlib import AsyncExitStack
from loguru import logger
import msgspec

from .definitions import CRC, FRAME, KEEP_ALIVE


class SocketClient:
    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self.crc = CRC()
        self.tg: asyncio.TaskGroup | None = None
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None
        self.terminate = False
        self.tasks: set[asyncio.Task] = set()
        self.tx_q: asyncio.Queue[bytes] = asyncio.Queue()
        self.rx_q: asyncio.Queue[dict] = asyncio.Queue()

    async def __aenter__(self) -> "SocketClient":
        self.terminate = False
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)

    async def __aexit__(self, *args) -> None:
        self.terminate = True
        self.writer.close()
        await self.writer.wait_closed()

    async def reader_task(self):
        while self.terminate is False:
            header = FRAME.copy()
            await self.reader.readuntil(FRAME)
            header += await self.reader.readexactly(3)
            if self.crc._check_crc8(header[:4], header[4]) is False:
                logger.error("CRC8 ERROR!")
                continue
            msg_len = int.from_bytes(header[2:4], "little")
            payload = await self.reader.readexactly(msg_len)
            try:
                self.rx_q.put_nowait(msgspec.json.decode(payload))
            except msgspec.DecodeError:
                pass

    async def writer_task(self):
        while self.terminate is False:
            try:
                payload = await asyncio.wait_for(self.tx_q.get(), 15)
            except TimeoutError:
                payload = KEEP_ALIVE
                # continue
            header = FRAME.copy() + len(payload).to_bytes(2, "little")
            header += bytes([self.crc.calc_blk_crc8(header)])
            self.writer.write(header + payload)
            await self.writer.drain()
            logger.trace(f"written {payload}")

    async def main_task(self):
        try:
            async with AsyncExitStack() as stack:
                await stack.enter_async_context(self)
                coros = [
                    self.writer_task(),
                    self.reader_task(),
                ]
                await asyncio.gather(*coros)
        except asyncio.exceptions.IncompleteReadError:
            logger.info("Socket closed from the server side")


async def test():
    client = SocketClient("127.0.0.1", 8888)
    await client.main_task()


if __name__ == "__main__":
    asyncio.run(test())

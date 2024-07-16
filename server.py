import asyncio
from asyncio.exceptions import IncompleteReadError
from contextlib import AsyncExitStack
from functools import partial
from time import time


from loguru import logger
import msgspec

from .definitions import CRC, FRAME, KEEP_ALIVE


class ClientHandler:
    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        rx_q: asyncio.Queue[dict],
    ) -> None:
        self.id: float = time()
        self.crc = CRC()
        self.reader = reader
        self.writer = writer
        self.terminate = False
        self.timeout_manager: asyncio.Timeout | None = None
        self.tasks: set[asyncio.Task] = set()
        self.tx_q: asyncio.Queue[bytes] = asyncio.Queue()
        self.rx_q = rx_q

    async def reader_task(self):
        loop = asyncio.get_running_loop()
        while self.terminate is False:
            header = FRAME.copy()
            await self.reader.readuntil(FRAME)
            header += await self.reader.readexactly(3)
            if self.crc._check_crc8(header[:4], header[4]) is False:
                logger.error("CRC8 ERROR!")
                continue
            msg_len = int.from_bytes(header[2:4], "little")
            payload = await self.reader.readexactly(msg_len)
            self.timeout_manager.reschedule(loop.time() + 60)

            if payload == KEEP_ALIVE:
                logger.trace(f"Keep alive from id: {self.id}")
                continue

            try:
                msg = msgspec.json.decode(payload)
                logger.trace(f"{msg = }, from id: {self.id}")
                await self.rx_q.put(msg)
            except msgspec.DecodeError:
                logger.error(payload)

    async def writer_task(self):
        while self.terminate is False:
            payload = await self.tx_q.get()
            header = FRAME.copy() + len(payload).to_bytes(2, "little")
            header += bytes([self.crc.calc_blk_crc8(header)])
            msg = header + payload
            self.writer.write(msg)
            await self.writer.drain()

    async def check_timeout(self):
        async with asyncio.timeout(60) as manager:
            self.timeout_manager = manager
            await asyncio.Future()

    async def __aenter__(self) -> "ClientHandler":
        pass

    async def __aexit__(self, *args):
        self.writer.close()
        await self.writer.wait_closed()
        self.terminate = True
        logger.info(f"Client handler terminated: {self.id}")

    async def main_task(self):
        try:
            async with AsyncExitStack() as stack:
                await stack.enter_async_context(self)
                coros = [
                    self.check_timeout(),
                    self.writer_task(),
                    self.reader_task(),
                ]
                await asyncio.gather(*coros)
        except TimeoutError:
            logger.error(f"{self.id = } Timed out")
        except IncompleteReadError:
            logger.error(f"{self.id = } disconnected")


class SocketServer:
    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self.tg: asyncio.TaskGroup | None = None
        self.client_handlers: set[ClientHandler] = set()
        self.rx_q: asyncio.Queue[dict] = asyncio.Queue(10)

    async def accept_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        handler = ClientHandler(reader, writer, self.rx_q)
        self.client_handlers.add(handler)
        task = self.tg.create_task(handler.main_task(), name=f"handler-{handler.id}")
        task.add_done_callback(partial(self.discard_handler, handler))
        logger.info(f"client accepted: {handler.id = }")

    def discard_handler(self, handler: ClientHandler, *_):
        self.client_handlers.discard(handler)

    async def main_task(self):
        async with asyncio.TaskGroup() as tg:
            self.tg = tg
            tg.create_task(self.serve(), name="server")

    async def broadcast(self, msg: str):
        for handler in self.client_handlers:
            handler.tx_q.put(msg)

    async def serve(self):
        server = await asyncio.start_server(self.accept_client, "0.0.0.0", 8888)
        logger.info(f"Serving socket server {self.host}:{self.port} ")
        async with server:
            await server.serve_forever()


async def test():
    from sys import stderr

    logger.remove()
    logger.add(stderr, level="TRACE")
    server = SocketServer("127.0.0.1", 8888)
    await server.main_task()


if __name__ == "__main__":
    asyncio.run(test())

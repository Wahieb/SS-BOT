import logging

from aiohttp import web

from .routes import routes

log = logging.getLogger(__name__)


class StreamServer:
    """Wraps an aiohttp web application that serves Telegram file streams.

    Start / stop it alongside the Pyrogram client (see ScreenShotBot).
    """

    def __init__(self, client, host: str, port: int) -> None:
        self.host = host
        self.port = port

        self.app = web.Application()
        self.app["client"] = client
        self.app.add_routes(routes)

        self.runner = web.AppRunner(self.app)

    async def start(self) -> None:
        await self.runner.setup()
        await web.TCPSite(self.runner, self.host, self.port).start()
        log.info("Stream server listening on http://%s:%s", self.host, self.port)

    async def stop(self) -> None:
        await self.runner.cleanup()
        log.info("Stream server stopped")

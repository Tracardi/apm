import asyncio
import signal


class MainLoop:

    def __init__(self, coro):
        loop = asyncio.get_event_loop()
        self.main_task = loop.create_task(self.main(coro))
        for sig in [signal.SIGINT, signal.SIGTERM]:
            loop.add_signal_handler(sig, self.main_task.cancel)
        loop.set_exception_handler(self._exception_handler)
        loop.run_until_complete(self.main_task)

    async def main(self, coro):
        await coro()

    def _exception_handler(self, loop, context):
        loop.default_exception_handler(context)
        self.main_task.cancel()

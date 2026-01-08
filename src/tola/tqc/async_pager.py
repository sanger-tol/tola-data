import asyncio


class AsyncQueryPager:
    def __init__(self, output):
        self.queue = asyncio.Queue()
        self.output = output
        self.row_count = 0

    async def fetch_query_pages(self):
        queue = self.queue
        for i in range(3):
            await asyncio.sleep(0)
            await queue.put(f"item {i}")
            self.row_count += 1
            print(f"Produced item {i}")
        queue.shutdown()

    async def do_output(self):
        queue = self.queue
        output = self.output
        while True:
            try:
                item = await queue.get()
            except asyncio.queues.QueueShutDown:
                break
            output(item)
            queue.task_done()

    async def gather_queue(self):
        await asyncio.gather(
            self.fetch_query_pages(),
            self.do_output(),
        )

    def run(self):
        asyncio.run(self.gather_queue())
        return self.row_count

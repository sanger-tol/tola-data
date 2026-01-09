import asyncio


class AsyncQueryPagerReuseError(Exception):
    """Raised if an `AsyncQueryPager` is attempted to be reused"""


class AsyncQueryPager:
    """Uses an `asyncio.Queue` to manage fetching data from from the provided
    async `query_itr` and calling the `output` function."""

    def __init__(self, *, query_itr=None, output=None, queue_size=1000):
        self.__queue = asyncio.Queue(maxsize=queue_size)
        self.__query_itr = query_itr
        self.__output = output
        self.__row_count = 0

    async def __fetch_query_pages(self):
        queue = self.__queue
        itr = self.__query_itr

        async for page in itr:
            try:
                for item in page:
                    await queue.put(item)
                    self.__row_count += 1
            except asyncio.queues.QueueShutDown:
                break
        queue.shutdown()

    async def __do_output(self):
        queue = self.__queue
        output = self.__output
        while True:
            try:
                item = await queue.get()
            except asyncio.queues.QueueShutDown:
                break
            queue.task_done()
            if not output(item):
                # Output failed, so shut down the queue.
                queue.shutdown()

    async def __gather_queue(self):
        await asyncio.gather(
            self.__fetch_query_pages(),
            self.__do_output(),
        )

    def run(self):
        if self.__row_count is None:
            msg = f"Cannot re-use {self.__class__.__name__} for a second query"
            raise AsyncQueryPagerReuseError(msg)

        asyncio.run(self.__gather_queue())

        # Return the count of the number of rows fetched
        n = self.__row_count
        self.__row_count = None
        return n

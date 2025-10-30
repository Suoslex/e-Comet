import asyncio

from typing import Any, Callable, Awaitable, Sequence


async def async_execute(
        func: Callable[..., Awaitable],
        args_list: Sequence[Sequence[Any]],
        max_workers: int = 10
) -> list[Any]:
    """
    Executes passed coroutine function concurrently using args_list arguments.
    To put it simply, it creates max_worker tasks, which will go over all
    elements of args_lists and execute func using every args.

    Parameters
    ----------
    func: Callable[..., Awaitable]
        Coroutine function to execute with given list of args.
    args_list: Sequence[Sequence[Any]]
        List of arguments to parameterize func.
    max_workers: int
        Maximum number of concurrent tasks working on the execution.

    Returns
    ----------
    list[Any]
        Result list of every func call for given args_list.
    """
    queue = asyncio.Queue()
    workers_count = min(max_workers, len(args_list))
    args_list += [None] * workers_count
    for args in args_list:
        await queue.put(args)

    async def _worker():
        results = []
        while True:
            args = await queue.get()
            if args is None:
                break
            results.append(await func(*args))
            queue.task_done()
        return results
    loop = asyncio.get_running_loop()
    tasks = []
    for i in range(workers_count):
        tasks.append(loop.create_task(_worker()))
    return [
        result
        for task_result in await asyncio.gather(*tasks)
        for result in task_result
    ]

import asyncio
from asyncio import Future
from datetime import datetime
from typing import Coroutine
from typing import Final
from typing import List
from typing import Optional

import nest_asyncio

from tecton_client.exceptions import TectonClientError

DATETIME_FORMATS: Final[List[str]] = ["%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ"]


def parse_string_to_isotime(datetime_string: str) -> Optional[datetime]:
    """Parse a string into a datetime object.

    Args:
        datetime_string (str): The string to parse to datetime.

    Returns:
        Optional[datetime]: The parsed datetime object if the string is parsed successfully, else None.

    """
    for format in DATETIME_FORMATS:
        try:
            return datetime.strptime(datetime_string, format)
        except ValueError:
            pass
    return None


def asyncio_run(coro: Coroutine) -> Optional[Future]:
    """Run the given asynchronous coroutine using the appropriate event loop.

    This method uses the `asyncio.get_event_loop()` to get the current event loop,
    and if it's not available, it creates a new event loop with `asyncio.new_event_loop()`.
    It then runs the given coroutine using `loop.run_until_complete(coro)`.

    If the loop is already running, it uses `asyncio.run()` to run the coroutine.
    This function must be called from outside any existing event loop to avoid conflicts.

    Args:
        coro (Coroutine): The asynchronous coroutine to be executed.

    Returns:
        Optional[Future]: The result of the asynchronous coroutine if the event loop is running, else None.

    Raises:
        TectonClientError: If the event loop throws any errors apart from there being no current event loop in thread.

    """
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError as e:
        if "no current event loop in thread" in str(e):
            loop = asyncio.new_event_loop()
            return loop.run_until_complete(coro)
        else:
            raise TectonClientError from e
    else:
        nest_asyncio.apply(loop)
        return asyncio.run(coro)

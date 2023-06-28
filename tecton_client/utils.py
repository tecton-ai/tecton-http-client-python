from datetime import datetime
from typing import Final
from typing import List

from tecton_client.exceptions import TectonClientError

DATETIME_FORMATS: Final[List[str]] = ["%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ"]


def parse_time(datetime_string: str) -> datetime:
    """Parse a datetime string into a datetime object.

    Args:
        datetime_string (str): The datetime string to parse.

    Returns:
        datetime: The parsed datetime object.

    Raises:
        TectonClientError: If the datetime string is invalid.

    """
    for format in DATETIME_FORMATS:
        try:
            return datetime.strptime(datetime_string, format)
        except ValueError:
            pass

    message = f"Invalid datetime string: {datetime_string}. Please contact Tecton for assistance."
    raise TectonClientError(message)

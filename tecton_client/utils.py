from datetime import datetime
from typing import Final
from typing import List


DATETIME_FORMATS: Final[List[str]] = ["%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ"]


def parse_string_to_isotime(datetime_string: str) -> datetime:
    """Parse a string into a datetime object.

    Args:
        datetime_string (str): The datetime string to parse.

    Returns:
        datetime: The parsed datetime object.

    Raises:
        ValueError: If the datetime string is invalid.

    """
    for format in DATETIME_FORMATS:
        try:
            return datetime.strptime(datetime_string, format)
        except ValueError:
            pass
    raise ValueError

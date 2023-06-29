from datetime import datetime
from typing import Final
from typing import List
from typing import Optional


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

from dataclasses import dataclass
from datetime import timedelta
from typing import Optional


@dataclass
class TectonClientOptions:
    """Class to represent the Tecton Client options.

    Examples:
        >>> options = TectonClientOptions(connect_timeout=timedelta(seconds=10),
        ...     keepalive_expiry=timedelta(seconds=600))
        >>> tecton_client = TectonClient(url, api_key, client_options=options)

    Attributes:
        connect_timeout (timedelta): (Optional) The maximum amount of time to wait until a socket connection to the
            requested host is established. Defaults to 2.0 seconds.
        read_timeout (timedelta): (Optional) The maximum duration to wait for a chunk of data to be received (for
            example, a chunk of the response body). Defaults to 2.0 seconds.
        keepalive_expiry (Optional[timedelta]): (Optional) The time limit on idle keep-alive connections in seconds,
            or None for no limits. Defaults to 300 seconds (5 minutes).
        max_connections (Optional[int]): (Optional) The maximum number of allowable connections, or None for no limits.
            Defaults to 10.

    """

    connect_timeout: timedelta = timedelta(seconds=2.0)
    read_timeout: timedelta = timedelta(seconds=2.0)
    keepalive_expiry: Optional[timedelta] = timedelta(seconds=300)
    max_connections: Optional[int] = 10

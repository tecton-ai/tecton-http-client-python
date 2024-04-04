import httpx


class TectonHttpException(Exception):
    """Base class for exceptions raised when the Tecton API returns an error response."""

    STATUS_CODE = 0

    def __init__(self, status_code: int, reason_phrase: str, message: str):
        """Create a TectonHttpException"""
        self.status_code: int
        self.message = f"{status_code} {reason_phrase}: {message}"
        super().__init__(self.message)


class BadRequestError(TectonHttpException):
    """Raised when the Tecton API returns a 400 Bad Request error."""

    STATUS_CODE = 400


class UnauthorizedError(TectonHttpException):
    """Raised when the Tecton API returns a 401 Unauthorized error."""

    STATUS_CODE = 401


class ForbiddenError(TectonHttpException):
    """Raised when the Tecton API returns a 403 Forbidden error."""

    STATUS_CODE = 403


class NotFoundError(TectonHttpException):
    """Raised when the Tecton API returns a 404 Not Found error."""

    STATUS_CODE = 404


class ResourceExhaustedError(TectonHttpException):
    """Raised when the Tecton API returns a 429 Resources Exhausted error."""

    STATUS_CODE = 429


class ServiceUnavailableError(TectonHttpException):
    """Raised when the Tecton API returns a 503 Service Unavailable error."""

    STATUS_CODE = 503


class GatewayTimeoutError(TectonHttpException):
    """Raised when the Tecton API returns a 504 Gateway Timeout error."""

    STATUS_CODE = 504


_HTTP_ERRORS: dict = {error.STATUS_CODE: error for error in TectonHttpException.__subclasses__()}


def convert_exception(httpx_exception: httpx.HTTPStatusError) -> TectonHttpException:
    """Convert a httpx.HTTPStatusError into a TectonHttpException for better message formatting"""
    status_code = httpx_exception.response.status_code
    exception_class = _HTTP_ERRORS.get(status_code)
    if exception_class:
        message = httpx_exception.response.json().get("message")
        return exception_class(
            status_code=status_code, reason_phrase=httpx_exception.response.reason_phrase, message=message
        )
    else:
        return TectonHttpException(status_code, httpx_exception.response.reason_phrase, "")

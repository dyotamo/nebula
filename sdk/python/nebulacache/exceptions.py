"""SDK exception hierarchy for transport, protocol, and server response failures."""

class NebulaCacheError(Exception):
    """Base exception raised by the NebulaCache client."""


class CacheConnectionError(NebulaCacheError):
    """Transport failure, pool acquisition timeout, or use of a closed client.

    Wrapped socket errors are available through __cause__.
    """


class CacheProtocolError(NebulaCacheError):
    """Invalid response framing, version, status, size, or successful PING payload."""


class CacheResponseError(NebulaCacheError):
    """Unsuccessful server operation or unexpected authentication result.

    The status attribute retains the numeric wire status: 0 OK, 1 NOT_FOUND,
    2 INVALID_ARGUMENT, 3 INTERNAL_ERROR, 4 UNAUTHORIZED, or 5 FORBIDDEN.
    Status 0 can occur when authentication returns an unexpected payload.
    The supplied message is included in the exception text alongside the status;
    GET and DELETE handle NOT_FOUND without raising this exception.
    """

    def __init__(self, status: int, message: str) -> None:
        self.status = status
        super().__init__(f"NebulaCache returned status {status}: {message}")

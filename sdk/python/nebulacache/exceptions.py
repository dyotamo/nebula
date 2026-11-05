class NebulaCacheError(Exception):
    """Base exception raised by the NebulaCache client."""


class CacheConnectionError(NebulaCacheError):
    """Raised when the client cannot connect to a NebulaCache node."""


class CacheProtocolError(NebulaCacheError):
    """Raised when a response does not conform to the NebulaCache protocol."""


class CacheResponseError(NebulaCacheError):
    """Raised when a NebulaCache node reports an unsuccessful operation."""

    def __init__(self, status: int, message: str) -> None:
        self.status = status
        super().__init__(f"NebulaCache returned status {status}: {message}")

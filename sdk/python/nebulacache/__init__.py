"""Export NebulaCache and its exception hierarchy.

NebulaCacheError is the common base for CacheConnectionError, CacheProtocolError,
and CacheResponseError, which distinguish transport, framing, and server failures.
"""

from .client import NebulaCache
from .exceptions import (
    CacheConnectionError,
    CacheProtocolError,
    CacheResponseError,
    NebulaCacheError,
)

__all__ = [
    "CacheConnectionError",
    "CacheProtocolError",
    "CacheResponseError",
    "NebulaCache",
    "NebulaCacheError",
]

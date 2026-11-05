package io.nebulacache;

public final class CacheProtocolException extends NebulaCacheException {
    private static final long serialVersionUID = 1L;

    public CacheProtocolException(String message) {
        super(message);
    }

    public CacheProtocolException(String message, Throwable cause) {
        super(message, cause);
    }
}

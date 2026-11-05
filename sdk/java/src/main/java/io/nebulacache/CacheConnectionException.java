package io.nebulacache;

public final class CacheConnectionException extends NebulaCacheException {
    private static final long serialVersionUID = 1L;

    public CacheConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}

package io.nebulacache;

/**
 * Indicates a transport failure, acquisition timeout, or interrupted pool/backoff wait.
 */
public final class CacheConnectionException extends NebulaCacheException {
    private static final long serialVersionUID = 1L;

    /**
     * Creates an exception with a message and underlying cause.
     *
     * @param message failure description
     * @param cause underlying failure, or null if unknown
     */
    public CacheConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}

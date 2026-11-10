package io.nebulacache;

/**
 * Indicates a malformed or unsupported response, including an invalid PING payload.
 */
public final class CacheProtocolException extends NebulaCacheException {
    private static final long serialVersionUID = 1L;

    /**
     * Creates an exception with a message.
     *
     * @param message failure description
     */
    public CacheProtocolException(String message) {
        super(message);
    }

    /**
     * Creates an exception with a message and underlying cause.
     *
     * @param message failure description
     * @param cause underlying failure, or null if unknown
     */
    public CacheProtocolException(String message, Throwable cause) {
        super(message, cause);
    }
}

package io.nebulacache;

/**
 * Base unchecked exception for NebulaCache transport, protocol, and server failures.
 */
public class NebulaCacheException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    /**
     * Creates an exception with a message.
     *
     * @param message failure description
     */
    public NebulaCacheException(String message) {
        super(message);
    }

    /**
     * Creates an exception with a message and underlying cause.
     *
     * @param message failure description
     * @param cause underlying failure, or null if unknown
     */
    public NebulaCacheException(String message, Throwable cause) {
        super(message, cause);
    }
}

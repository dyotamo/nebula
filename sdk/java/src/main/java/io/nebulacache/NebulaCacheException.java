package io.nebulacache;

public class NebulaCacheException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public NebulaCacheException(String message) {
        super(message);
    }

    public NebulaCacheException(String message, Throwable cause) {
        super(message, cause);
    }
}

package io.nebulacache;

/**
 * Indicates a server error status or failed authentication.
 * A missing key is handled normally by {@link NebulaCache#get(String)} and
 * {@link NebulaCache#delete(String)} rather than raising this exception.
 */
public final class CacheResponseException extends NebulaCacheException {
    private static final long serialVersionUID = 1L;

    /** Numeric protocol status associated with this failure. */
    private final int status;

    /**
     * Creates an exception whose message includes the numeric status and supplied detail.
     *
     * @param status protocol status code
     * @param message failure detail
     */
    public CacheResponseException(int status, String message) {
        super("NebulaCache returned status " + status + ": " + message);
        this.status = status;
    }

    /**
     * Returns the status supplied when this exception was constructed.
     *
     * @return numeric status code; defined errors are 1 (not found), 2 (invalid argument),
     *     3 (internal error), 4 (unauthorized), and 5 (forbidden)
     */
    public int getStatus() {
        return status;
    }
}

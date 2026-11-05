package io.nebulacache;

public final class CacheResponseException extends NebulaCacheException {
    private static final long serialVersionUID = 1L;

    private final int status;

    public CacheResponseException(int status, String message) {
        super("NebulaCache returned status " + status + ": " + message);
        this.status = status;
    }

    public int getStatus() {
        return status;
    }
}

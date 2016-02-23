package io.ddf2.datasource.filesystem.fileformat;

/**
 * Created by sangdn on 1/4/16.
 */
public class FileFormatResolverException extends Exception {
    /**
     * Constructs a new exception with the specified detail message.  The
     * cause is not initialized, and may subsequently be initialized by
     * a call to {@link #initCause}.
     *
     * @param message the detail message. The detail message is saved for
     *                later retrieval by the {@link #getMessage()} method.
     */
    public FileFormatResolverException(String message) {
        super(message);
    }
}

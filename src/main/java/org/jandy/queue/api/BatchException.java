package org.jandy.queue.api;

/**
 * Can be thrown by a {@link BatchMessageProcessor} when a problem occurs processing a batch. The
 * batch will not be retried and simply allows a custom message to be logged
 */
public class BatchException extends RuntimeException {

    public BatchException(String message, Throwable cause) {
        super(message, cause);
    }

    public BatchException(Throwable cause) {
        super(cause);
    }
}

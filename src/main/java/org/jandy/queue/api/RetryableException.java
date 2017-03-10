package org.jandy.queue.api;

import org.jandy.queue.core.QueueBatchReader;

/**
 * Signals a transient error to the {@link QueueBatchReader} so that the batch is
 * rolled back and retried
 */
public class RetryableException extends RuntimeException {

    public RetryableException(String message, Throwable cause) {
        super(message, cause);
    }

    public RetryableException(Throwable cause) {
        super(cause);
    }
}

package org.jandy.queue.api;

import org.jandy.queue.core.QueueBatchReader;

import java.util.List;

/**
 * Processes batches read by {@link QueueBatchReader}
 */
public interface BatchMessageProcessor {

    /**
     * Called when a batch has been read from the queue. Note the batch may be incomplete
     * if the {@link QueueBatchReader#timeoutSeconds} was reached but it will never be empty.
     * If processing a batch can fail then throwing either {@link RetryableException} or
     * {@link BatchException} will determine retry behaviour
     *
     * @param batch the batch read from the queue
     * @throws Exception on failing a batch
     */
    void processBatch(List<String> batch) throws Exception;
}

package org.jandy.queue.example;

import org.apache.commons.io.FileUtils;
import org.jandy.queue.core.QueueBatchReader;
import org.jandy.queue.core.QueueServer;
import org.jandy.queue.core.QueueWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.QueueConnection;
import java.io.File;

/**
 * Example usage that starts an embedded queue server writing and reading messages.
 */
public class QueueExample {

    private static final Logger log = LoggerFactory.getLogger(QueueExample.class);

    private static final int BATCH_DELAY_SECONDS = 30;
    private static final int BATCH_SIZE = 50;
    private static final String QUEUE_NAME = "my-queue";
    private static final int THREADS = 2;

    public static void main(String[] args) {
        // start the queue server
        File location = FileUtils.getFile(FileUtils.getTempDirectory(), "artemis");
        QueueServer server = new QueueServer(location.getAbsolutePath()).start();
        log.info("Server started using location: {}", location);

        // create the queue
        QueueConnection connection = server.getConnection();
        server.createQueue(QUEUE_NAME);

        // create the producer
        QueueWriter queueWriter = new QueueWriter(connection, QUEUE_NAME);
        BasicObjectProducer producer = new BasicObjectProducer(queueWriter);

        // create the consumer
        QueueBatchReader queueReader = new QueueBatchReader(connection, QUEUE_NAME, THREADS);
        queueReader.setBatchSize(BATCH_SIZE);
        queueReader.setTimeout(BATCH_DELAY_SECONDS);
        queueReader.start(new BasicObjectProcessor());

        // produce a few messages and stop
        producer.produce(new BasicObject("a", 1));
        producer.produce(new BasicObject("b", 2));
        producer.produce(new BasicObject("c", 3));

        queueReader.stop();
        server.stop();
    }
}

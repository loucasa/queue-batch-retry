package org.jandy.queue.core;

import org.apache.activemq.artemis.api.core.ActiveMQInterruptedException;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.management.JMSManagementHelper;
import org.apache.activemq.artemis.reader.MessageUtil;
import org.jandy.queue.api.BatchMessageProcessor;
import org.jandy.queue.api.RetryableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueRequestor;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.base.Throwables.propagate;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.activemq.artemis.api.core.management.ResourceNames.CORE_QUEUE;
import static org.apache.activemq.artemis.api.core.management.ResourceNames.JMS_QUEUE;

/**
 * Spawns threads to read messages from the queue in batches and pass those batches onto
 * the processor when start is called
 */
public class QueueBatchReader {

    private static final Logger log = LoggerFactory.getLogger(QueueBatchReader.class);
    private static final String SHUTDOWN = "shutdown_request";

    private final QueueConnection connection;
    private final Queue queue;
    private final int threads;
    private final ExecutorService executor;

    private volatile boolean stop = false;

    private int timeoutSeconds = 10;
    private int batchSize = 50;
    private int retryWarningLimit = 0;

    /**
     * Creates the reader for the queue, {@link #start(BatchMessageProcessor)} must be called
     * to start reading messages
     *
     * @param connection    queue connection obtained from server
     * @param queueName     queue name (created using the server)
     * @param threads       number of batch reader threads
     */
    public QueueBatchReader(QueueConnection connection, String queueName, int threads) {
        this.connection = connection;
        this.threads = threads;
        this.queue = ActiveMQJMSClient.createQueue(queueName);
        this.executor = Executors.newFixedThreadPool(threads);
    }

    public void start(BatchMessageProcessor processor) {
        Callable<Void> batchReaderCallable = () -> {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageConsumer consumer = session.createConsumer(queue);
            while (!stop) {
                List<String> batch = new ArrayList<>(batchSize);
                try {
                    int messagesRetried = readBatch(consumer, batch);
                    if (messagesRetried > 0) {
                        log.error("{} / {} messages retried at least {} times - there could be a problem downstream",
                                  messagesRetried, batch.size(), retryWarningLimit);
                    }
                    // batch may be empty after timeout - no need to pass on to processor
                    if (!batch.isEmpty()) {
                        log.trace("Queue [{}] - Processing batch - [{}] items", queue, batch.size());
                        processor.processBatch(batch);
                    }
                    session.commit();
                } catch (ActiveMQInterruptedException e) {
                    log.trace("Received interrupt on queue [{}]", queue.getQueueName());
                    Thread.currentThread().interrupt();
                } catch (RetryableException e) {
                    log.info("Queue [{}] - error processing batch [{}] - rolling back for retry", queue, toString(batch), e);
                    session.rollback();
                } catch (Exception e) {
                    log.error("Queue [{}] - failed processing batch [{}]", queue, toString(batch), e);
                    session.commit();
                }
            }
            consumer.close();
            session.close();
            return null;
        };
        for (int i = 0; i < threads; i++) {
            executor.submit(batchReaderCallable);
        }
        log.debug("Batch queue reader started - threads={}, queue={}", threads, queue);
    }

    private int readBatch(MessageConsumer consumer, List<String> batch) throws JMSException {
        int messagesRetried = 0;
        for (int j = 0; j < batchSize; j++) {
            log.trace("Waiting for message on queue [{}]", queue);
            // casting is the most efficient way to get at the text body!
            Message message = consumer.receive(SECONDS.toMillis(timeoutSeconds));
            if (message == null || message.propertyExists(SHUTDOWN) || !(message instanceof TextMessage)) {
                log.trace("Ending batch - queue [{}] received message: {}", queue, message);
                break;
            }
            TextMessage textMessage = (TextMessage) message;
            int retryCount = textMessage.getIntProperty(MessageUtil.JMSXDELIVERYCOUNT);
            if (retryWarningLimit != 0 && retryCount > retryWarningLimit) {
                messagesRetried++;
            }
            log.trace("Queue [{}] - received message [deliverCount={}] : {}", queue, retryCount, textMessage);
            batch.add(textMessage.getText());
        }
        return messagesRetried;
    }

    public void stop() {
        stop = true;
        sendShutdownRequests();
        executor.shutdown();
        try {
            executor.awaitTermination(10, SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Send a shutdown request for each queue reader thread so that we process the remaining items
     * in a batch before shutting down
     */
    private void sendShutdownRequests() {
        try (
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(queue)
        ) {
            for (int i = 0; i < threads; i++) {
                Message message = session.createTextMessage();
                message.setBooleanProperty(SHUTDOWN, true);
                producer.send(message);
                log.debug("Sent shutdown request on queue [{}]", queue.getQueueName());
            }
        } catch (JMSException e) {
            log.error("Error creating shutdown request - will wait for timeout", e);
        }
    }

    /**
     * Utility method to warn when the queue sizes breaches a limit
     */
    public void checkServiceStatus(QueueServer server, QueueBatchReader queueReader, long warningLimit) {
        Queue queue = queueReader.getQueue();
        try {
            long queueSize = server.getQueueSize(queue);
            if (queueSize > warningLimit) {
                log.warn("Queue [{}] size={}, warning limit {}", queue.getQueueName(), queueSize, warningLimit);
            }
        } catch (Exception e) {
            log.error("Unable to determine queue size [{}]", queue, e);
        }
    }

    /**
     * Seems to cause a memory leak even though it follows the example (keeping it here in case of a fix):
     *
     * https://github.com/apache/activemq-artemis/blob/master/examples/features/standard/management/src/main/java/org/apache/activemq/artemis/jms/example/ManagementExample.java
     *
     * @deprecated use {@link QueueServer#getQueueSize(Queue)}
     */
    @SuppressWarnings("unused")
    public int getQueueSize() {
        try (
            QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE)
        ) {
            Queue managementQueue = ActiveMQJMSClient.createQueue("activemq.management");
            QueueRequestor requestor = new QueueRequestor(session, managementQueue);
            Message message = session.createTextMessage();
            String resource = format("%s%s%s", CORE_QUEUE, JMS_QUEUE, queue.getQueueName());
            JMSManagementHelper.putAttribute(message, resource, "messageCount");
            Message reply = requestor.request(message);
            requestor.close();
            return (int) (Integer) JMSManagementHelper.getResult(reply);
        } catch (Exception e) {
            throw propagate(e);
        }
    }

    /**
     * Maximum seconds to wait for a message before returning an incomplete batch - can be set
     * to zero to wait forever
     *
     * @param timeoutSeconds time to wait for complete batches
     */
    public void setTimeout(int timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds;
    }

    /**
     * Set the batch size
     *
     * @param batchSize number of messages to include in a batch
     */
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    /**
     * Logs an error when a message has been retried more than the number specified so that an
     * alert can be triggered
     *
     * @param retryWarningLimit number of retries before logging error or zero to disable
     */
    public void setRetryWarningLimit(int retryWarningLimit) {
        this.retryWarningLimit = retryWarningLimit;
    }

    public Queue getQueue() {
        return queue;
    }

    /**
     * String representation of the object taken from {@link Object#toString()}
     * (because ArrayList annoyingly overrides it!)
     */
    private String toString(Object o) {
        return o.getClass().getName() + "@" + Integer.toHexString(hashCode());
    }
}

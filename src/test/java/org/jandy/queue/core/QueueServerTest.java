package org.jandy.queue.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.jandy.queue.api.BatchException;
import org.jandy.queue.api.BatchMessageProcessor;
import org.jandy.queue.api.RetryableException;
import org.junit.After;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mockito.verification.VerificationMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Queue;
import javax.jms.QueueConnection;
import java.io.File;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Throwables.propagate;
import static com.jayway.awaitility.Awaitility.await;
import static com.jayway.awaitility.Duration.TEN_SECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

/**
 * Integration test for the {@link QueueServer}
 */
public class QueueServerTest {

    private static final Logger log = LoggerFactory.getLogger(QueueServerTest.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private final String queueName = "test";

    private QueueBatchReader queueReader;
    private QueueWriter queueWriter;

    private final QueueServer server;
    private final BatchMessageProcessor batchProcessor;

    public QueueServerTest() throws Exception {
        // setup location ready for automatically created queue related files
        File location = FileUtils.getFile(FileUtils.getTempDirectory(), "artemis");
        FileUtils.deleteDirectory(location);
        server = new QueueServer(location.getAbsolutePath()).start();
        server.createQueue(queueName);

        batchProcessor = mock(BatchMessageProcessor.class);
    }

    @After
    public void after() throws Exception {
        queueReader.stop();
        server.stop();
    }

    @Test
    public void testSimpleBatch() throws Exception {
        start(2, 5, 1);

        // send one event
        produce();

        // check we processed it
        assertProduceInvoked(timeout(SECONDS.toMillis(3)).times(1));
    }

    @Test
    public void testDurability() throws Exception {
        // simulate slow batch processing by sleeping
        doNothing()
            .doAnswer(i -> {
                log.debug("Mock producer sleeping...");
                Thread.sleep(1500);
                return null;
            })
            .when(batchProcessor).processBatch(anyListOf(String.class))
        ;
        // small batch size so test is simpler only needing a few events
        start(10, 1, 1);

        // send 3 events - these will be split over 3 batches
        produce();
        produce();

        // check we processed 2 batches then simulate restart
        assertProduceInvoked(timeout(SECONDS.toMillis(1)).times(2));

        queueReader.stop();
        produce();
        // todo: restart the embedded server also
        //after();
        //server.start();

        reset(batchProcessor);
        start(1, 5, 1);

        // check we processed the remaining batch after starting back up
        assertProduceInvoked(timeout(SECONDS.toMillis(3)).times(1));
    }

    @Test
    public void testRetry() throws Exception {
        // throw a retryable exception to retry the batch
        setProducerException(new RetryableException(new RuntimeException("deliberate")));

        // batch size and retry warning of 1
        start(1, 1, 1);
        queueReader.setRetryWarningLimit(1);

        // send 1 events - that's 1 batch
        produce();

        // check we retry the batch - producer invoked twice instead of once
        assertProduceInvoked(timeout(SECONDS.toMillis(5)).times(2));
        // note there should be an error logged so that we are alerted of a downstream problem but keep retrying
    }

    @Test
    public void testRecordNotProcessedAfterException() throws Exception {
        // throw exception processing the first batch only
        setProducerException(new BatchException(new RuntimeException("deliberate")));

        // start the producer with a small batch size
        start(1, 1, 1);

        // send 1 events - that's 1 batch
        produce();

        // check we didn't retry the batch - producer invoked once
        assertProduceInvoked(timeout(SECONDS.toMillis(5)).times(1));
    }

    @Test
    public void testMultiThread() throws Exception {
        RequestCountingAnswer countingAnswer = new RequestCountingAnswer();
        doAnswer(countingAnswer).when(batchProcessor).processBatch(anyListOf(String.class));

        // start the producer
        start(2, 5, 10);

        // send multiple events
        for (int i = 0; i < 100; i++) {
            produce();
        }

        // due to threading we can't be sure how many batches we'll get so count individual events sent
        await().atMost(TEN_SECONDS).until(countingAnswer::getCount, equalTo(100));
    }

    private void start(int timeoutSeconds, int batchSize, int threads) throws Exception {
        QueueConnection connection = server.getConnection();

        queueReader = new QueueBatchReader(connection, queueName, threads);
        queueReader.setBatchSize(batchSize);
        queueReader.setTimeout(timeoutSeconds);
        queueReader.start(batchProcessor);
        Queue queue = queueReader.getQueue();

        queueWriter = new QueueWriter(connection, queueName);
    }

    private void produce() {
        try {
            queueWriter.write(mapper.writeValueAsString(""));
        } catch (JsonProcessingException e) {
            propagate(e);
        }
    }

    private void assertProduceInvoked(VerificationMode verificationMode) {
        try {
            verify(batchProcessor, verificationMode).processBatch(anyListOf(String.class));
        } catch (Exception e) {
            propagate(e);
        }
    }

    private void setProducerException(Exception e) throws Exception {
        doThrow(e).doNothing().when(batchProcessor).processBatch(anyListOf(String.class));
    }

    private static class RequestCountingAnswer implements Answer<Void> {

        private AtomicInteger count = new AtomicInteger();

        @Override
        public Void answer(InvocationOnMock invocation) throws Throwable {
            count.getAndAdd(invocation.getArgumentAt(0, Collection.class).size());
            return null;
        }

        int getCount() {
            return count.get();
        }
    }
}
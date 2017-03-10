package org.jandy.queue.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jandy.queue.core.QueueWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Throwables.propagate;

/**
 * A background thread will read batches from the file and send them on.
 *
 * When persisting events before sending we need to serialize them, java serialization is slow so we use
 * ObjectMapper
 */
public class BasicObjectProducer {

    private static final Logger log = LoggerFactory.getLogger(BasicObjectProducer.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private final QueueWriter queueWriter;

    BasicObjectProducer(QueueWriter queueWriter) {
        this.queueWriter = queueWriter;
    }

    public void produce(BasicObject value) {
        try {
            log.trace("Writing object {}", value);
            queueWriter.write(mapper.writeValueAsString(value));
        } catch (JsonProcessingException e) {
            propagate(e);
        }
    }
}

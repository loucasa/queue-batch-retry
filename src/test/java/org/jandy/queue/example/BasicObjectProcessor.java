package org.jandy.queue.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.jandy.queue.api.BatchMessageProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Throwables.propagate;

/**
 * {@link BatchMessageProcessor} that deserializes objects read from the queue
 */
class BasicObjectProcessor implements BatchMessageProcessor {
    private static final Logger log = LoggerFactory.getLogger(BasicObjectProcessor.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private final List<BasicObject> values;

    BasicObjectProcessor() {
        values = new ArrayList<>();
    }

    @Override
    public void processBatch(List<String> batch) throws IOException {
        // deserialize from queue wrapper to avro object - don't fail the whole batch if any one fails

        for (String item : batch) {
            try {
                BasicObject wrappedRecord = readObject(item);
                values.add(wrappedRecord);
                log.info("Read item {}", wrappedRecord);
            } catch (Exception e) {
                log.error("Error - parsing record for: {}", item, e);
            }
        }
    }

    public List<BasicObject> getValues() {
        return values;
    }

    private BasicObject readObject(String json) {
        try {
            return mapper.readValue(json, BasicObject.class);
        } catch (IOException e) {
            throw propagate(e);
        }
    }
}

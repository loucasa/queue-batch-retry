package org.jandy.queue.example;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Simple object for queue serialization example
 */
class BasicObject {
    private final String stringProperty;
    private final int intProperty;

    @JsonCreator
    public BasicObject(@JsonProperty("stringProperty") String stringProperty,
                       @JsonProperty("intProperty") int intProperty) {
        this.stringProperty = stringProperty;
        this.intProperty = intProperty;
    }

    public String getStringProperty() {
        return stringProperty;
    }

    public int getIntProperty() {
        return intProperty;
    }
}

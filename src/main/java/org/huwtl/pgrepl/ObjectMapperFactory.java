package org.huwtl.pgrepl;

import com.fasterxml.jackson.databind.ObjectMapper;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

public class ObjectMapperFactory {
    public static ObjectMapper objectMapper() {
        return new ObjectMapper()
                .disable(FAIL_ON_UNKNOWN_PROPERTIES);
    }
}

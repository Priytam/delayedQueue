package com.github.priytam;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.priytam.event.Event;
import com.github.priytam.event.EventEnvelope;
import java.io.IOException;
import java.io.UncheckedIOException;

public class SerializerDeserializer {

    private final ObjectMapper mapper;

    public SerializerDeserializer(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public String serialize(EventEnvelope<? extends Event> envelope) {
        try {
            return mapper.writeValueAsString(envelope);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    public <T extends Event> EventEnvelope<T> deserialize(Class<T> eventType, String rawEnvelope) {
        JavaType envelopeType = mapper.getTypeFactory().constructParametricType(EventEnvelope.class, eventType);
        try {
            return mapper.readValue(rawEnvelope, envelopeType);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}

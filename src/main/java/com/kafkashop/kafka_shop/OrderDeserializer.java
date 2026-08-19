package com.kafkashop.kafka_shop;

import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class OrderDeserializer implements Deserializer<Order> {

    private final ObjectMapper objectMapper =
            new ObjectMapper();

    @Override
    public Order deserialize(String topic, byte[] data) {

        try {

            if (data == null) {
                return null;
            }

            return objectMapper.readValue(
                    data,
                    Order.class
            );

        } catch (Exception e) {

            throw new RuntimeException(
                    "Error deserializing Order",
                    e
            );
        }
    }
}

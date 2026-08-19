package com.kafkashop.kafka_shop;

import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class OrderSerializer implements Serializer<Order> {

    private final ObjectMapper objectMapper =
            new ObjectMapper();

    @Override
    public byte[] serialize(String topic, Order order) {

        try {

            if (order == null) {
                return null;
            }

            return objectMapper.writeValueAsBytes(order);

        } catch (Exception e) {

            throw new RuntimeException(
                    "Error serializing Order",
                    e
            );
        }
    }
}

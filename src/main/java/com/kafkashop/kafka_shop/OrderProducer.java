package com.kafkashop.kafka_shop;

import java.util.Properties;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class OrderProducer {

	public static void main(String[] args) throws InterruptedException, ExecutionException, JsonProcessingException {
		//Properties is a normal Java class for storing configuration as key/value pairs.
		Properties properties = new Properties();

		properties.put("bootstrap.servers", "localhost:9092");
		
		properties.put(
			    "key.serializer",
			    "org.apache.kafka.common.serialization.StringSerializer"
			);

			properties.put(
			    "value.serializer",
			    OrderSerializer.class.getName()
			);
			
		Producer<String, Order> producer = new KafkaProducer<>(properties);
		//ObjectMapper objectMapper = new ObjectMapper(); //Used for Jackson
		//We don't need ObjectMapper anymore since we're passing in the order object directly into the producer object above
		
		for (int orderId = 101; orderId <= 110; orderId++) {

		    String key = String.valueOf(orderId);

		    Order order = new Order(
		            orderId,
		            "Mechanical Keyboard",
		            2,
		            79.99,
		            "customer@example.com"
		    );
		    
		    //String orderJson = objectMapper.writeValueAsString(order); //Converting the java object into a JSON formatted string

		    ProducerRecord<String, Order> record =
		            new ProducerRecord<>(
		                    "orders",
		                    key,
		                    order
		            );

		    var metadata = producer.send(record).get();

		    System.out.println(
		            "Order " + orderId +
		            " → Partition " + metadata.partition() +
		            ", Offset " + metadata.offset()
		    );
		}

		//System.out.println("Order successfully stored in Kafka!");
		//Subsection inside a kafka that allows connections to be established by the producer and consumers
		//System.out.println("Topic: " + metadata.topic());
		//A partition is one ordered log of records inside a Kafka topic
		//System.out.println("Partition: " + metadata.partition());
		//The position of a record within a particular partition.
		//System.out.println("Offset: " + metadata.offset());

		producer.close();

		//System.out.println("Order sent!");
	}

}

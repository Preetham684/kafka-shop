package com.kafkashop.kafka_shop;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class OrderProducer {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		//Properties is a normal Java class for storing configuration as key/value pairs.
		Properties properties = new Properties();

		properties.put("bootstrap.servers", "localhost:9092");
		
		properties.put(
			    "key.serializer",
			    "org.apache.kafka.common.serialization.StringSerializer"
			);

			properties.put(
			    "value.serializer",
			    "org.apache.kafka.common.serialization.StringSerializer"
			);
			
		Producer<String, String> producer = new KafkaProducer<>(properties);
		
		String order = "103,Roses,6,20";
		
		ProducerRecord<String, String> record =
		        new ProducerRecord<>(
		            "orders",
		            "103",
		            order
		        );
		
		var metadata = producer.send(record).get();

		System.out.println("Order successfully stored in Kafka!");
		//Subsection inside a kafka that allows connections to be established by the producer and consumers
		System.out.println("Topic: " + metadata.topic());
		//A partition is one ordered log of records inside a Kafka topic
		System.out.println("Partition: " + metadata.partition());
		//The position of a record within a particular partition.
		System.out.println("Offset: " + metadata.offset());

		producer.close();

		//System.out.println("Order sent!");
	}

}

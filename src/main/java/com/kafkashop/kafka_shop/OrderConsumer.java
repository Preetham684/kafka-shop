package com.kafkashop.kafka_shop;

import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.List;
import java.time.Duration;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class OrderConsumer {

	public static void main(String[] args) {
		System.out.println("1. Starting OrderConsumer...");
		Properties properties = new Properties();

		properties.put(
		    "bootstrap.servers",
		    "localhost:9092"
		);

		properties.put(
		    "key.deserializer",
		    "org.apache.kafka.common.serialization.StringDeserializer"
		);

		properties.put(
		    "value.deserializer",
		    "org.apache.kafka.common.serialization.StringDeserializer"
		);

		properties.put(
		    "group.id",
		    "order-service"
		);
		

		Consumer<String, String> consumer = new KafkaConsumer<>(properties);

		System.out.println("2. KafkaConsumer created...");

		consumer.subscribe(List.of("orders"));

		System.out.println("3. Subscribed to orders. Waiting...");
		
		while (true) {

		    ConsumerRecords<String, String> records =
		            consumer.poll(Duration.ofMillis(100));

		    for (ConsumerRecord<String, String> record : records) {

		        System.out.println("Order received!");
		        System.out.println("Key: " + record.key());
		        System.out.println("Value: " + record.value());
		        System.out.println("Partition: " + record.partition());
		        System.out.println("Offset: " + record.offset());
		        System.out.println("------------------------");
		    }
		}
	}

}

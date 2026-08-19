package com.kafkashop.kafka_shop;

import java.util.Properties;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.List;
import java.time.Duration;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class OrderConsumer {

	public static void main(String[] args) throws JsonMappingException, JsonProcessingException {
		System.out.println("1. Starting OrderConsumer...");
		
		String consumerName = args.length > 0 ? args[0] : "Unknown Consumer";

		System.out.println("Starting " + consumerName);
		
		Properties properties = new Properties();

		properties.put(
		    "bootstrap.servers",
		    "localhost:9092"
		);

		properties.put(
		    "key.deserializer",
		    "org.apache.kafka.common.serialization.StringDeserializer"
		);

		//We only need to deserialize the value and not the key, since we need the value as an object while the key can remain as a string and thereby uses the built in StringSerializer
		properties.put(
		    "value.deserializer",
		    OrderDeserializer.class.getName()
		);

		properties.put(
		    "group.id",
		    "order-service"
		);
		

		Consumer<String, Order> consumer = new KafkaConsumer<>(properties);
		//ObjectMapper objectMapper = new ObjectMapper();

		System.out.println("2. KafkaConsumer created...");

		consumer.subscribe(List.of("orders"));

		System.out.println("3. Subscribed to orders. Waiting...");
		
		while (true) {

		    ConsumerRecords<String, Order> records =
		            consumer.poll(Duration.ofMillis(100));

		    for (ConsumerRecord<String, Order> record : records) {
		    	
//		    	Order order =
//		    	        objectMapper.readValue(
//		    	                record.value(),
//		    	                Order.class
//		    	        ); Tells Jackson to convert JSON to a Java object
		    	
		    	Order order = record.value();

		    	System.out.println("Consumer: " + consumerName);
		        System.out.println("Order received!");
		        
		        System.out.println("Order ID: " + order.getOrderId());
		        System.out.println("Product: " + order.getProduct());
		        System.out.println("Quantity: " + order.getQuantity());
		        System.out.println("Price: $" + order.getPrice());
		        System.out.println("Customer: " + order.getCustomerEmail());

		        System.out.println("Partition: " + record.partition());
		        System.out.println("Offset: " + record.offset());
		        System.out.println("------------------------");
		    }
		}
	}

}

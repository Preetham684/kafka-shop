# KafkaShop

KafkaShop is an **event-driven order processing system** built with Java and Apache Kafka.

The goal is to build a realistic backend system where services can process order events independently, scale through Kafka partitions and consumer groups, and support real-time event processing.

## Architecture

```text
                  Order Producer
                        |
                        v
                +---------------+
                | Apache Kafka  |
                | Topic: orders |
                |  P0  P1  P2   |
                +-------+-------+
                        |
              +---------+---------+
              |                   |
              v                   v
       Order Consumer      Analytics Consumer
        order-service       analytics-service
```

Orders are published as structured JSON events:

```json
{
  "orderId": 101,
  "product": "Mechanical Keyboard",
  "quantity": 2,
  "price": 79.99,
  "customerEmail": "customer@example.com"
}
```

## What's Implemented

* Kafka producers and consumers using the native Java client
* Multi-partition `orders` topic
* Consumer groups and parallel event processing
* Independent consumer groups for order processing and analytics
* Custom `Order` serialization/deserialization using Jackson
* Partition and offset tracking
* Consumer-group rebalancing
* Basic event schema evolution

## Tech Stack

**Java · Apache Kafka · Maven · Jackson · Git**

## Roadmap

KafkaShop will evolve into a distributed order-processing backend with:

* Spring Boot REST API
* Payment and Inventory services
* PostgreSQL persistence
* Retry and dead-letter handling
* Idempotent event processing
* Docker
* Integration testing
* Real-time order analytics

## Goal

The project demonstrates how Kafka can act as the **event backbone of a distributed system**, allowing services to communicate asynchronously, process high-throughput event streams, scale independently, and recover from failures.

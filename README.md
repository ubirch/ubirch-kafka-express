# UBIRCH KAFKA EXPRESS

Abstractions for creating consumers and producers.

## Consumer Runner

The purpose of the consumer runner is to provide a simple an straightforward abstraction
for creating and managing a kafka consumer.

The principal elements of the consumer runner environment are:

1. **Configs:** A convenience to manage the configuration keys that are used to initialize the kafka consumer.

2. **ConsumerRecordsController:** Represents the the type that is actually processes the consumer records. 
The consumer doesn't care about how it is processed, it can be with Futures, Actors, as long as the result type matches.

3. **ConsumerRunner:** Represents a Consumer Runner for a Kafka Consumer. 
 It supports back-pressure using the pause/unpause. The pause duration is amortized.
 It supports plugging rebalance listeners.
 It supports autocommit and not autocommit.
 It supports commit attempts.
 It supports to "floors" for exception management. This is allows to escalate exceptions.
 It supports callbacks on the most relevant events.

4. **ProcessResult:** Represents the result that is expected result for the consumption. This is helpful to return the consumer record and an identifiable record.
This type is usually extended to support customized data.

5. **WithMetrics:** Adds prometheus support to consumer runners.

6. **Consumers:** There are out-of-the box consumers. A String and Bytes Consumers.

7. **ConsumerBasicConfigs:** This is helper trait that creates/defines a basic config for a consumer.

8. **WithConsumerShutdownHook:** This a helper that defines a hook to shut down a consumer.

## Producer Runner

The purpose of the producer runner is to provide a simple and straightforward abstraction 
for creating a kafka producer.

The principal elements of the producer runner environment are:

1. **Configs:** A convenience to manage the configuration keys that are used to initialize the kafka producer.

2. **ProducerRunner:** Represents a simple definition for a kafka producer. It supports callback on the producer creation event

3. **ProducerBasicConfigs** This is helper trait that creates/defines a basic config for a producer.

4. **WithProducerShutdownHook:** This a helper that defines a hook to shut down a producer.

## Express Kafka Steps ##

1. To create an Express Kafka App you have to implement ExpressKafkaApp. This component trait takes 
two type parameters, K and V, K stands for Key and V for Value. You have to pass these parameters in 
depending on the kind of consumer and producer you would like to have.

2. You have to implement a couple of configuration values:
    
    * keyDeserializer
    * valueDeserializer
    * consumerTopics
    * consumerBootstrapServers
    * consumerGroupId
    * consumerMaxPollRecords
    * consumerGracefulTimeout
    * producerBootstrapServers
    * keySerializer
    * valueSerializer

3. You have to implement the business logic in the method called *process*. This method is basically 
called every time the consumer polls for new data. That's to say that the vector of consumer records is 
the data arriving in this configure topic.

4. Optionally, you are able to send -publish- data to different topics if necessary. You can do this 
with the method *send*.

5. You need to have a Kafka Server running. For more information, check https://kafka.apache.org/

## Import into project

```xml
        <dependency>
            <groupId>com.ubirch</groupId>
            <artifactId>ubirch-kafka-express</artifactId>
            <version>1.2.14-SNAPSHOT</version>
        </dependency>
```

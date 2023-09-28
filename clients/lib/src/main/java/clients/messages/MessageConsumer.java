package clients.messages;

import clients.airport.consumers.AbstractInteractiveShutdownConsumer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Simple example consumer, to present the basic Kafka consumer API. It's generally equivalent to
 * the kafka-consumer.sh script.
 */
public class MessageConsumer extends AbstractInteractiveShutdownConsumer {

  public void run() {
    Properties props = new Properties();
    props.put("bootstrap.servers", MessageProducer.BOOTSTRAP_SERVERS);
    props.put("group.id", "ping-consumers");
    props.put("enable.auto.commit", "true");

    try (Consumer<String, String> consumer =
        new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer())) {
      consumer.subscribe(Collections.singleton(MessageProducer.TOPIC_NAME));
      System.out.println("Waiting for events...");

      while (!done) {
        // Wait up to 2s to get event
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
        for (ConsumerRecord<String, String> record : records) {
          System.out.printf(
              "Event received: offset = %d, key = %s, value = %s%n",
              record.offset(), record.key(), record.value());
        }
      }
    }
  }

  public static void main(String[] args) {
    new MessageConsumer().runUntilEnterIsPressed(System.in);
  }
}

package clients.airport.consumers.stuck;

import clients.airport.AirportProducer;
import clients.airport.consumers.AbstractInteractiveShutdownConsumer;
import clients.messages.MessageProducer;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Detects started checkins which get stuck in the middle due to an OUT_OF_ORDER event, and raises
 * them as events.
 */
public class StuckCustomerConsumer extends AbstractInteractiveShutdownConsumer {

  private static final String TOPIC_STUCK_CUSTOMERS = "selfservice-stuck-customers";

  public void run() {
    Properties props = new Properties();
    props.put("bootstrap.servers", MessageProducer.BOOTSTRAP_SERVERS);
    props.put("group.id", "stuck-customers-simple");
    props.put("enable.auto.commit", "true");

    Properties producerProps = new Properties();
    props.put("bootstrap.servers", MessageProducer.BOOTSTRAP_SERVERS);

    Set<Integer> startedCheckins = new HashSet<>();

    try (KafkaConsumer<Integer, AirportProducer.TerminalInfo> consumer =
            new KafkaConsumer<>(
                props, new IntegerDeserializer(), new AirportProducer.TerminalInfoDeserializer());
        KafkaProducer<Integer, String> producer =
            new KafkaProducer<>(producerProps, new IntegerSerializer(), new StringSerializer())) {
      consumer.subscribe(
          List.of(
              AirportProducer.TOPIC_COMPLETED,
              AirportProducer.TOPIC_CANCELLED,
              AirportProducer.TOPIC_CHECKIN,
              AirportProducer.TOPIC_OUTOFORDER));

      while (!done) {
        ConsumerRecords<Integer, AirportProducer.TerminalInfo> records =
            consumer.poll(Duration.ofSeconds(1));
        for (ConsumerRecord<Integer, AirportProducer.TerminalInfo> record : records) {
          switch (record.topic()) {
            case AirportProducer.TOPIC_CHECKIN:
              startedCheckins.add(record.key());
              break;
            case AirportProducer.TOPIC_COMPLETED:
            case AirportProducer.TOPIC_CANCELLED:
              startedCheckins.remove(record.key());
              break;
            case AirportProducer.TOPIC_OUTOFORDER:
              if (startedCheckins.contains(record.key())) {
                System.out.printf("Machine with key %s is stuck!%n", record.key());
                producer.send(new ProducerRecord<>(TOPIC_STUCK_CUSTOMERS, record.key(), "stuck"));
                startedCheckins.remove(
                    record.key()); // machine can get "fixed" and go out of order again
              }
              break;
          }
        }
      }
    }
  }

  public static void main(String[] args) {
    new StuckCustomerConsumer().runUntilEnterIsPressed(System.in);
  }
}

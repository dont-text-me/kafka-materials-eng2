package clients.airport.consumers.totals;

import clients.airport.AirportProducer;
import clients.airport.consumers.AbstractInteractiveShutdownConsumer;
import clients.messages.MessageProducer;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;

/**
 * Consumer that reports total numbers of started, completed, and cancelled checkins. The first
 * version is very simplistic and won't handle rebalancing. This overall computation wouldn't scale
 * well anyway, as it doesn't apply any windows or split the input in any particular way.
 */
public class TotalCheckinsConsumer extends AbstractInteractiveShutdownConsumer {

  public void run() {
    Properties props = new Properties();
    props.put("bootstrap.servers", MessageProducer.BOOTSTRAP_SERVERS);
    props.put("group.id", "total-checkins");
    props.put("enable.auto.commit", "true");

    int started = 0, completed = 0, cancelled = 0;

    try (KafkaConsumer<Integer, AirportProducer.TerminalInfo> consumer =
        new KafkaConsumer<>(
            props, new IntegerDeserializer(), new AirportProducer.TerminalInfoDeserializer())) {
      consumer.subscribe(
          List.of(
              AirportProducer.TOPIC_COMPLETED,
              AirportProducer.TOPIC_CANCELLED,
              AirportProducer.TOPIC_CHECKIN));

      var lastUpdateTime = Instant.now();

      while (!done) {
        ConsumerRecords<Integer, AirportProducer.TerminalInfo> records =
            consumer.poll(Duration.ofSeconds(1));
        for (ConsumerRecord<Integer, AirportProducer.TerminalInfo> record : records) {
          switch (record.topic()) {
            case AirportProducer.TOPIC_CHECKIN:
              started++;
              break;
            case AirportProducer.TOPIC_COMPLETED:
              completed++;
              break;
            case AirportProducer.TOPIC_CANCELLED:
              cancelled++;
              break;
          }
          var recordTimeStamp = Instant.ofEpochMilli(record.timestamp());
          if (Duration.between(lastUpdateTime, recordTimeStamp).toSeconds() == 5) {
            System.out.printf(
                "Timestamp: %s, stats: started: %d, Completed: %d, Cancelled: %d%n",
                lastUpdateTime, started, completed, cancelled);
            lastUpdateTime = recordTimeStamp;
          }
        }
      }
    }
  }

  public static void main(String[] args) {
    new TotalCheckinsConsumer().runUntilEnterIsPressed(System.in);
  }
}

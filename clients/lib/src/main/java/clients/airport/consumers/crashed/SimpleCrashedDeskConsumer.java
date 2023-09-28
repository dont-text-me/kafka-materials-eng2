package clients.airport.consumers.crashed;

import clients.airport.AirportProducer;
import clients.airport.consumers.AbstractInteractiveShutdownConsumer;
import clients.messages.MessageProducer;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;

/** Consumer which will print out terminals that haven't a STATUS event in a while. */
public class SimpleCrashedDeskConsumer extends AbstractInteractiveShutdownConsumer {

  public void run() {
    Properties props = new Properties();
    props.put("bootstrap.servers", MessageProducer.BOOTSTRAP_SERVERS);
    props.put("group.id", "crashed-desks-simple");

    // Kafka will auto-commit every 5s based on the last poll() call
    props.put("enable.auto.commit", "true");

    Map<Integer, Instant> lastHeartbeat = new TreeMap<>();
    Set<Integer> crashedMachines = new HashSet<>();

    try (KafkaConsumer<Integer, AirportProducer.TerminalInfo> consumer =
        new KafkaConsumer<>(
            props, new IntegerDeserializer(), new AirportProducer.TerminalInfoDeserializer())) {
      consumer.subscribe(Collections.singleton(AirportProducer.TOPIC_STATUS));

      while (!done) {
        ConsumerRecords<Integer, AirportProducer.TerminalInfo> records =
            consumer.poll(Duration.ofSeconds(1));
        for (ConsumerRecord<Integer, AirportProducer.TerminalInfo> record : records) {
          // update last timestamps
          lastHeartbeat.put(record.key(), Instant.ofEpochMilli(record.timestamp()));
        }

        var currentTimestamp = Instant.now();

        lastHeartbeat.forEach(
            (recordKey, recordHeartbeat) -> {
              if (Duration.between(recordHeartbeat, currentTimestamp).toSeconds() == 12) {
                System.out.printf("Machine with key %s has crashed!%n", recordKey);
                crashedMachines.add(recordKey);
              }
            });
      }
      System.out.printf("%s machines crashed in total %n", crashedMachines.size());
    }
  }

  public static void main(String[] args) {
    new SimpleCrashedDeskConsumer().runUntilEnterIsPressed(System.in);
  }
}

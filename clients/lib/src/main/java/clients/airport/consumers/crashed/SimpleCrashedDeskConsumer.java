package clients.airport.consumers.crashed;

import clients.airport.AirportProducer;
import clients.airport.consumers.AbstractInteractiveShutdownConsumer;
import clients.messages.MessageProducer;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;

/** Consumer which will print out terminals that haven't a STATUS event in a while. */
public class SimpleCrashedDeskConsumer extends AbstractInteractiveShutdownConsumer {

  public void run() {
    Properties props = new Properties();
    props.put("bootstrap.servers", MessageProducer.BOOTSTRAP_SERVERS);
    props.put("group.id", "crashed-desks-simple");

    // Kafka will auto-commit every 5s based on the last poll() call
    props.put("enable.auto.commit", "true");

    //    Map<Integer, Instant> lastHeartbeat = new TreeMap<>();
    Table<Integer, Integer, Instant> lastHeartbeat = HashBasedTable.create();
    // terminalId -> partition
    Map<Integer, Integer> crashedMachines = new TreeMap<>();

    try (KafkaConsumer<Integer, AirportProducer.TerminalInfo> consumer =
        new KafkaConsumer<>(
            props, new IntegerDeserializer(), new AirportProducer.TerminalInfoDeserializer())) {

      ConsumerRebalanceListener listener =
          new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
              System.out.println(
                  "Lost partitions" + partitions.stream().map(TopicPartition::partition).toList());
              partitions.forEach(partition -> lastHeartbeat.row(partition.partition()).clear());
              crashedMachines
                  .values()
                  .removeIf(
                      partition ->
                          partitions.stream()
                              .map(TopicPartition::partition)
                              .toList()
                              .contains(partition));
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
              System.out.println(
                  "Gained partitions"
                      + partitions.stream().map(TopicPartition::partition).toList());
              consumer.seekToBeginning(partitions);
            }
          };

      consumer.subscribe(Collections.singleton(AirportProducer.TOPIC_STATUS), listener);

      while (!done) {
        ConsumerRecords<Integer, AirportProducer.TerminalInfo> records =
            consumer.poll(Duration.ofSeconds(1));
        for (ConsumerRecord<Integer, AirportProducer.TerminalInfo> record : records) {
          // update last timestamps
          lastHeartbeat.put(
              record.partition(), record.key(), Instant.ofEpochMilli(record.timestamp()));
          crashedMachines.remove(record.key(), record.partition());
        }

        var currentTimestamp = Instant.now();

        lastHeartbeat
            .rowMap()
            .forEach(
                (partition, heartbeatMap) -> {
                  heartbeatMap.forEach(
                      (machineId, heartbeatInstant) -> {
                        if (Duration.between(heartbeatInstant, currentTimestamp).toSeconds()
                            == 12) {
                          // System.out.printf("Machine with key %s has crashed!%n", recordKey);
                          crashedMachines.put(machineId, partition);
                        }
                      });
                });
        System.out.printf("%s machines are currently crashed %n", crashedMachines.size());
      }
    }
  }

  public static void main(String[] args) {
    new SimpleCrashedDeskConsumer().runUntilEnterIsPressed(System.in);
  }
}

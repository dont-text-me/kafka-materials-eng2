package clients.airport.consumers.windows;

import static clients.airport.Utils.getTerminalArea;

import clients.airport.AirportProducer;
import clients.airport.consumers.AbstractInteractiveShutdownConsumer;
import clients.messages.MessageProducer;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;

/**
 * Computes windowed checkin counts over each general area of the airport (defined as the hundreds
 * digit of the terminal ID, e.g. terminal 403 is in area 4).
 *
 * <p>The first version compute counts correctly if we use more than one consumer in the group, and
 * it will forget events if we rebalance. We will fix these issues later on.
 *
 * <p>After implementing practical 3, the above issue is solved. However, it requires the use of
 * {@link clients.airport.AreaPartitioner} to work correctly.
 */
public class WindowedAreaCheckinsConsumer extends AbstractInteractiveShutdownConsumer {

  private Duration windowSize = Duration.ofSeconds(30);

  public void run() {
    Properties props = new Properties();
    props.put("bootstrap.servers", MessageProducer.BOOTSTRAP_SERVERS);
    props.put("group.id", "windowed-area-stats");
    props.put("enable.auto.commit", "true");

    Map<Integer, TimestampSlidingWindow> windowCheckinsByArea = new HashMap<>();

    try (KafkaConsumer<Integer, AirportProducer.TerminalInfo> consumer =
        new KafkaConsumer<>(
            props, new IntegerDeserializer(), new AirportProducer.TerminalInfoDeserializer())) {
      ConsumerRebalanceListener listener =
          new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
              System.out.println(
                  "Lost partitions" + partitions.stream().map(TopicPartition::partition).toList());
              partitions.forEach(
                  partition ->
                      windowCheckinsByArea.remove(
                          partition.partition())); // Note: assumes that partition id = area id (see
              // AreaPartitioner)
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
              System.out.println(
                  "Gained partitions"
                      + partitions.stream().map(TopicPartition::partition).toList());
              consumer.seekToBeginning(partitions);
            }
          };

      consumer.subscribe(Collections.singleton(AirportProducer.TOPIC_CHECKIN), listener);

      while (!done) {
        ConsumerRecords<Integer, AirportProducer.TerminalInfo> records =
            consumer.poll(Duration.ofSeconds(1));
        for (ConsumerRecord<Integer, AirportProducer.TerminalInfo> record : records) {
          if (windowCheckinsByArea.containsKey(getTerminalArea(record.key()))) {
            windowCheckinsByArea
                .get(getTerminalArea(record.key()))
                .add(Instant.ofEpochMilli(record.timestamp()));
          } else {
            windowCheckinsByArea.put(getTerminalArea(record.key()), new TimestampSlidingWindow());
          }
        }

        windowCheckinsByArea.forEach(
            (area, window) -> {
              Integer count = window.windowCount(Instant.now().minus(windowSize), Instant.now());
              System.out.printf(
                  "%s checkins in area %s in the last %s seconds%n", count, area, windowSize);
            });
      }
    }
  }

  public static void main(String[] args) {
    new WindowedAreaCheckinsConsumer().runUntilEnterIsPressed(System.in);
  }
}

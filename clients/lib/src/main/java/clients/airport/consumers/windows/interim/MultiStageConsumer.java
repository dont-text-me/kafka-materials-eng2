package clients.airport.consumers.windows.interim;

import static clients.airport.Utils.getTerminalArea;

import clients.airport.AirportProducer;
import clients.airport.consumers.windows.TimestampSlidingWindow;
import clients.messages.MessageProducer;
import com.google.common.collect.Sets;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;

/**
 * This is the starting code for a multi-stage consumer, which first re-keys checkins by area into
 * an interim topic, and then has another thread consume those events.
 *
 * <p>Computes windowed checkins over each general area of the airport (defined as the hundreds
 * digit of the terminal ID).
 */
public class MultiStageConsumer {

  private static final String TOPIC_AREA_CHECKIN = "selfservice-area-checkin";
  private static final String GROUP_ID = "windowed-area-multistage";

  private final Duration windowSize = Duration.ofSeconds(30);
  private volatile boolean done = false;

  private void runAreaTask() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, MessageProducer.BOOTSTRAP_SERVERS);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, MessageProducer.BOOTSTRAP_SERVERS);

    /*
     * Consume checkins and produce an event whose key is the area and whose
     * value is the original record timestamp into the topic referenced by the
     * TOPIC_AREA_CHECKIN constant.
     *
     * Exit the while loop when the 'done' field becomes true.
     */

    try (KafkaConsumer<Integer, AirportProducer.TerminalInfo> consumer =
            new KafkaConsumer<>(
                props, new IntegerDeserializer(), new AirportProducer.TerminalInfoDeserializer());
        KafkaProducer<Integer, Long> producer =
            new KafkaProducer<>(producerProps, new IntegerSerializer(), new LongSerializer())) {
      consumer.subscribe(Collections.singleton(AirportProducer.TOPIC_CHECKIN));

      while (!done) {
        ConsumerRecords<Integer, AirportProducer.TerminalInfo> records =
            consumer.poll(Duration.ofSeconds(1));
        for (ConsumerRecord<Integer, AirportProducer.TerminalInfo> record : records) {
          //          System.out.printf(
          //              "Producer: sending message about area %d%n",
          // getTerminalArea(record.key()));
          producer.send(
              new ProducerRecord<>(
                  TOPIC_AREA_CHECKIN, getTerminalArea(record.key()), record.timestamp()));
        }
      }
    }
  }

  private void runWindowTask() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, MessageProducer.BOOTSTRAP_SERVERS);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

    Map<Integer, Set<Integer>> partitionToAreas =
        new HashMap<>(); // We can do this because the message keys are airport areas, so same area
    // will go in the same partitions if a hash is used
    Map<Integer, TimestampSlidingWindow> areasToWindows = new HashMap<>();

    /*
     * Consume events from the new interim topic (TOPIC_AREA_CHECKIN) and use
     * those to compute windowed per-area counts, similarly to our old consumer.
     *
     * The next step would be to use a rebalance listener, forgetting old results
     * and seeking to the beginning when we are assigned new partitions.
     */

    try (KafkaConsumer<Integer, Long> consumer =
        new KafkaConsumer<>(props, new IntegerDeserializer(), new LongDeserializer())) {

      ConsumerRebalanceListener listener =
          new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
              System.out.println(
                  "Lost partitions" + partitions.stream().map(TopicPartition::partition).toList());
              partitions.forEach(partition -> partitionToAreas.remove(partition.partition()));

              areasToWindows
                  .keySet()
                  .removeIf(
                      it ->
                          partitionToAreas.values().stream()
                              .noneMatch(areasSet -> areasSet.contains(it)));
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
              System.out.println(
                  "Gained partitions"
                      + partitions.stream().map(TopicPartition::partition).toList());
            }
          };

      consumer.subscribe(Collections.singleton(TOPIC_AREA_CHECKIN), listener);

      while (!done) {
        ConsumerRecords<Integer, Long> records = consumer.poll(Duration.ofSeconds(1));
        for (ConsumerRecord<Integer, Long> record : records) {
          //          System.out.printf("Consumer: received message about area %d%n", record.key());

          partitionToAreas.merge(
              record.partition(),
              Sets.newHashSet(record.key()),
              (oldSet, newSet) -> {
                oldSet.addAll(newSet);
                return oldSet;
              });

          Instant checkinTimestamp =
              Instant.ofEpochMilli(record.value()); // the timestamp of the original checkin
          // (not the interim topic message)
          if (areasToWindows.containsKey(record.key())) {
            areasToWindows.get(record.key()).add(checkinTimestamp);
          } else {
            areasToWindows.put(record.key(), new TimestampSlidingWindow());
          }
        }
        // Store the window start timestamp per partition
        Map<TopicPartition, Long> partitionToTimestamp = new HashMap<>();
        Instant windowStart = Instant.now().minus(windowSize);
        consumer
            .partitionsFor(TOPIC_AREA_CHECKIN)
            .forEach(
                partitionInfo ->
                    partitionToTimestamp.put(
                        new TopicPartition(TOPIC_AREA_CHECKIN, partitionInfo.partition()),
                        windowStart.toEpochMilli()));

        // Get the offsets for the partitions at the window start timestamp
        Map<TopicPartition, OffsetAndTimestamp> partitionToOffsetTimestamp =
            consumer.offsetsForTimes(partitionToTimestamp);

        // Convert data to format accepted by the commit function
        Map<TopicPartition, OffsetAndMetadata> p2o = new HashMap<>();
        partitionToOffsetTimestamp.forEach(
            (partition, offsetAndTimestamp) -> {
              if (offsetAndTimestamp != null) {
                p2o.put(partition, new OffsetAndMetadata(offsetAndTimestamp.offset()));
              }
            });

        // print current results
        areasToWindows.forEach(
            (area, window) -> {
              Integer count = window.windowCount(windowStart, Instant.now());
              System.out.printf(
                  "%s checkins in area %s in the last %s seconds%n", count, area, windowSize);
            });

        // commit
        consumer.commitAsync(
            p2o, (offsets, exception) -> System.out.println("Manual commit complete"));
      }
    }
  }

  public void run() throws IOException, InterruptedException {
    Thread tAreaTask = new Thread(this::runAreaTask, "AreaTask");
    Thread tWindowTask = new Thread(this::runWindowTask, "WindowTask");

    tAreaTask.start();
    tWindowTask.start();

    try (BufferedReader bR = new BufferedReader(new InputStreamReader(System.in))) {
      // We can press Enter to exit cleanly
      bR.readLine();
    } finally {
      done = true;
    }

    tAreaTask.join();
    tWindowTask.join();
  }

  public static void main(String[] args) {
    try {
      new MultiStageConsumer().run();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

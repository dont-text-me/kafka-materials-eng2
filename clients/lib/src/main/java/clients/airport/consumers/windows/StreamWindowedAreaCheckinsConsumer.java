package clients.airport.consumers.windows;

import static clients.airport.AirportProducer.TOPIC_CHECKIN;
import static clients.airport.Utils.getTerminalArea;

import clients.airport.AirportProducer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

public class StreamWindowedAreaCheckinsConsumer {
  private final Duration windowSize = Duration.ofSeconds(30);
  private final Duration gracePeriod = Duration.ofSeconds(60);

  public KafkaStreams run() {
    StreamsBuilder builder = new StreamsBuilder();

    Serde<AirportProducer.TerminalInfo> serde = new AirportProducer.TerminalInfoSerde();
    builder.stream(TOPIC_CHECKIN, Consumed.with(Serdes.Integer(), serde))
        .selectKey((k, v) -> getTerminalArea(k))
        .groupByKey(Grouped.with(Serdes.Integer(), serde))
        .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(windowSize, gracePeriod))
        .count()
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream()
        .print(Printed.toSysOut());

    Properties props = new Properties();
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AirportProducer.BOOTSTRAP_SERVERS);
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-status");

    KafkaStreams kStreams = new KafkaStreams(builder.build(), props);
    Runtime.getRuntime().addShutdownHook(new Thread(kStreams::close));
    kStreams.start();

    return kStreams;
  }

  public static void main(String[] args) {
    KafkaStreams kStreams = new StreamWindowedAreaCheckinsConsumer().run();

    // Shut down the application after pressing Enter in the Console
    try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in))) {
      br.readLine();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      kStreams.close();
    }
  }
}

package clients.airport.consumers.stuck;

import clients.airport.AirportProducer;
import clients.airport.Utils;
import clients.airport.consumers.stuck.processors.DeskStatus;
import clients.airport.consumers.stuck.processors.StuckCustomerProcessor;
import clients.airport.consumers.stuck.processors.TimestampWithStatus;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

public class StreamStuckCustomerConsumer {
  public static final String TOPIC_DESK_STUCK = "selfservice-stuck";

  public KafkaStreams run() {
    StreamsBuilder builder = new StreamsBuilder();
    Serde<AirportProducer.TerminalInfo> serde = new AirportProducer.TerminalInfoSerde();
    KStream<Integer, String> stream =
        builder.stream(
                List.of(
                    AirportProducer.TOPIC_COMPLETED,
                    AirportProducer.TOPIC_CANCELLED,
                    AirportProducer.TOPIC_CHECKIN,
                    AirportProducer.TOPIC_OUTOFORDER),
                Consumed.with(Serdes.Integer(), serde))
            .processValues(StuckCustomerProcessor::new)
            .groupByKey(
                Grouped.with(Serdes.Integer(), new Utils.GsonSerde<>(TimestampWithStatus.class)))
            .reduce(
                (acc, current) -> {
                  TimestampWithStatus older = TimestampWithStatus.min(acc, current);
                  TimestampWithStatus newer = TimestampWithStatus.max(acc, current);
                  if (older.status().equals(DeskStatus.STARTED)
                      && newer.status().equals(DeskStatus.OUT_OF_ORDER)) {
                    return new TimestampWithStatus(newer.timestamp(), DeskStatus.STUCK);
                  } else {
                    return newer;
                  }
                })
            .toStream()
            .filter((key, value) -> value.status().equals(DeskStatus.STUCK))
            .mapValues(
                (key, value) ->
                    "Desk %s has been reported as stuck at %s%n"
                        .formatted(
                            key,
                            Instant.ofEpochMilli(value.timestamp())
                                .atZone(ZoneOffset.UTC)
                                .format(DateTimeFormatter.ISO_DATE_TIME)));

    stream.to(TOPIC_DESK_STUCK, Produced.with(Serdes.Integer(), Serdes.String()));
    stream.print(Printed.toSysOut());

    Properties props = new Properties();
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AirportProducer.BOOTSTRAP_SERVERS);
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-stuck");

    KafkaStreams kStreams = new KafkaStreams(builder.build(), props);
    Runtime.getRuntime().addShutdownHook(new Thread(kStreams::close));
    kStreams.start();

    return kStreams;
  }

  public static void main(String[] args) {
    KafkaStreams kStreams = new StreamStuckCustomerConsumer().run();

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

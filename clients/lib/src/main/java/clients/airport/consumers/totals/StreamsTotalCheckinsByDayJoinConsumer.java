package clients.airport.consumers.totals;

import clients.airport.AirportProducer;
import clients.airport.consumers.totals.processors.DateProcessor;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Objects;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

public class StreamsTotalCheckinsByDayJoinConsumer {
  public KafkaStreams run() {
    StreamsBuilder builder = new StreamsBuilder();

    Serde<AirportProducer.TerminalInfo> serde = new AirportProducer.TerminalInfoSerde();
    KTable<String, Long>
        startedTable =
            builder.stream(AirportProducer.TOPIC_CHECKIN, Consumed.with(Serdes.Integer(), serde))
                .process(DateProcessor::new)
                .groupByKey(Grouped.with(Serdes.String(), serde))
                .count(),
        completedTable =
            builder.stream(AirportProducer.TOPIC_COMPLETED, Consumed.with(Serdes.Integer(), serde))
                .process(DateProcessor::new)
                .groupByKey(Grouped.with(Serdes.String(), serde))
                .count(),
        cancelledTable =
            builder.stream(AirportProducer.TOPIC_CANCELLED, Consumed.with(Serdes.Integer(), serde))
                .process(DateProcessor::new)
                .groupByKey(Grouped.with(Serdes.String(), serde))
                .count();

    KTable<String, String> joinedTable =
        startedTable
            .outerJoin(
                completedTable,
                (started, completed) ->
                    String.format(
                        "Started: %s, Completed: %s",
                        Objects.requireNonNullElse(started, 0),
                        Objects.requireNonNullElse(completed, 0)))
            .outerJoin(
                cancelledTable,
                (intermediateJoinString, cancelled) ->
                    Objects.requireNonNullElse(intermediateJoinString, "Started: 0, Completed: 0")
                        + " Cancelled: "
                        + Objects.requireNonNullElse(cancelled, 0));

    joinedTable.toStream().print(Printed.toSysOut());

    Properties props = new Properties();
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AirportProducer.BOOTSTRAP_SERVERS);
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-totals-join");

    KafkaStreams kStreams = new KafkaStreams(builder.build(), props);
    Runtime.getRuntime().addShutdownHook(new Thread(kStreams::close));
    kStreams.start();

    return kStreams;
  }

  public static void main(String[] args) {
    KafkaStreams kStreams = new StreamsTotalCheckinsByDayJoinConsumer().run();

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

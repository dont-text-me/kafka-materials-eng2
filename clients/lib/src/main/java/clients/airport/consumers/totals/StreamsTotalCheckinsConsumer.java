package clients.airport.consumers.totals;

import clients.airport.AirportProducer;
import clients.airport.Utils;
import clients.airport.consumers.totals.processors.DateAndTopic;
import clients.airport.consumers.totals.processors.TimestampProcessor;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;

public class StreamsTotalCheckinsConsumer {
  public static final String TOPIC_CHECKINS_BY_DAY = "selfservice-checkins-by-day";

  public KafkaStreams run() {
    StreamsBuilder builder = new StreamsBuilder();

    Serde<AirportProducer.TerminalInfo> serde = new AirportProducer.TerminalInfoSerde();
    Utils.GsonSerde<DateAndTopic> processedSerde = new Utils.GsonSerde<>(DateAndTopic.class);
    KStream<DateAndTopic, Long> countStream =
        builder.stream(
                List.of(
                    AirportProducer.TOPIC_COMPLETED,
                    AirportProducer.TOPIC_CANCELLED,
                    AirportProducer.TOPIC_CHECKIN),
                Consumed.with(Serdes.Integer(), serde))
            .processValues(TimestampProcessor<Integer, AirportProducer.TerminalInfo>::new)
            .groupBy((k, v) -> v, Grouped.with(processedSerde, processedSerde))
            .count()
            .toStream();

    countStream.to(TOPIC_CHECKINS_BY_DAY);

    countStream.print(Printed.toSysOut());

    Properties props = new Properties();
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AirportProducer.BOOTSTRAP_SERVERS);
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-status");

    KafkaStreams kStreams = new KafkaStreams(builder.build(), props);
    Runtime.getRuntime().addShutdownHook(new Thread(kStreams::close));
    kStreams.start();

    return kStreams;
  }

  public static void main(String[] args) {
    KafkaStreams kStreams = new StreamsTotalCheckinsConsumer().run();

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

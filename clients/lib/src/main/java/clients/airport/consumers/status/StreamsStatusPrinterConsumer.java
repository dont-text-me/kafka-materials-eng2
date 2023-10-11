package clients.airport.consumers.status;

import clients.airport.AirportProducer;
import clients.airport.AirportProducer.TerminalInfo;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Printed;

/**
 * Example port of {@link StatusPrinterConsumer} to Kafka Streams, to be used to illustrate the
 * basics of the API in the lab.
 */
public class StreamsStatusPrinterConsumer {

  public KafkaStreams run() {
    // 1. Use StreamsBuilder to build the topology
    StreamsBuilder builder = new StreamsBuilder();

    // Consume the status updates from the TOPIC_STATUS (Integer key and TerminalInfo value)
    Serde<TerminalInfo> serde = new AirportProducer.TerminalInfoSerde();
    builder.stream(AirportProducer.TOPIC_STATUS, Consumed.with(Serdes.Integer(), serde))
        // Map each key-value to a formatted string
        .mapValues((k, v) -> String.format("Status from %s: %d sheets%n", k, v.paperLeft))
        // Print them out to the standard output
        .print(Printed.toSysOut());

    // 2. Configure the Properties (need bootstrap serves and app ID at least)
    Properties props = new Properties();
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AirportProducer.BOOTSTRAP_SERVERS);
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-status");

    // 3. Use the builder to create the low-level Topology, set up shutdown hook, and start the app
    KafkaStreams kStreams = new KafkaStreams(builder.build(), props);
    Runtime.getRuntime().addShutdownHook(new Thread(kStreams::close));
    kStreams.start();

    return kStreams;
  }

  public static void main(String[] args) {
    KafkaStreams kStreams = new StreamsStatusPrinterConsumer().run();

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

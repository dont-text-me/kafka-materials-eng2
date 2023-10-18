package clients.airport.consumers.crashed;

import clients.airport.AirportProducer;
import clients.airport.consumers.crashed.processors.CrashedDeskProcessor;
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
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

public class StreamCrashedDeskConsumer {
  public static final String CRASHED_STORE_NAME = "crashed-store";

  public KafkaStreams run() {
    StreamsBuilder builder = new StreamsBuilder();

    Serde<AirportProducer.TerminalInfo> serde = new AirportProducer.TerminalInfoSerde();
    StoreBuilder<KeyValueStore<Integer, Long>> store =
        Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore(CRASHED_STORE_NAME), Serdes.Integer(), Serdes.Long());

    builder.addStateStore(store).stream(
            AirportProducer.TOPIC_STATUS, Consumed.with(Serdes.Integer(), serde))
        .processValues(CrashedDeskProcessor::new, CRASHED_STORE_NAME);

    Properties props = new Properties();
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AirportProducer.BOOTSTRAP_SERVERS);
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-crashed");

    KafkaStreams kStreams = new KafkaStreams(builder.build(), props);
    Runtime.getRuntime().addShutdownHook(new Thread(kStreams::close));
    kStreams.start();

    return kStreams;
  }

  public static void main(String[] args) {
    KafkaStreams kStreams = new StreamCrashedDeskConsumer().run();

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

package clients.airport.consumers.crashed.processors;

import static clients.airport.consumers.crashed.StreamCrashedDeskConsumer.CRASHED_STORE_NAME;

import clients.airport.AirportProducer;
import java.time.Duration;
import java.time.Instant;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualFixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;

public class CrashedDeskProcessor
    extends ContextualFixedKeyProcessor<Integer, AirportProducer.TerminalInfo, Long> {

  private KeyValueStore<Integer, Long> store;

  @Override
  public void init(FixedKeyProcessorContext<Integer, Long> context) {
    super.init(context);
    this.store = context().getStateStore(CRASHED_STORE_NAME);
    context()
        .schedule(
            Duration.ofSeconds(5),
            PunctuationType.WALL_CLOCK_TIME,
            timestamp ->
                this.store
                    .all()
                    .forEachRemaining(
                        it -> {
                          if (Duration.between(Instant.ofEpochMilli(it.value), Instant.now())
                                  .toSeconds()
                              == 12) {
                            System.out.printf(
                                "Terminal with ID %s has not responded for 12 seconds!%n", it.key);
                          }
                        }));
  }

  @Override
  public void process(FixedKeyRecord record) {
    this.store.put((Integer) record.key(), record.timestamp());
  }
}

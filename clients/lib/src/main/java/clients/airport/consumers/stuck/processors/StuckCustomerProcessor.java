package clients.airport.consumers.stuck.processors;

import clients.airport.AirportProducer;
import org.apache.kafka.streams.processor.api.ContextualFixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

public class StuckCustomerProcessor
    extends ContextualFixedKeyProcessor<
        Integer, AirportProducer.TerminalInfo, TimestampWithStatus> {
  @Override
  public void process(FixedKeyRecord<Integer, AirportProducer.TerminalInfo> record) {
    String topic = context().recordMetadata().get().topic();

    DeskStatus status;

    switch (topic) {
      case AirportProducer.TOPIC_CANCELLED -> status = DeskStatus.CANCELLED;
      case AirportProducer.TOPIC_CHECKIN -> status = DeskStatus.STARTED;
      case AirportProducer.TOPIC_COMPLETED -> status = DeskStatus.COMPLETED;
      case AirportProducer.TOPIC_OUTOFORDER -> status = DeskStatus.OUT_OF_ORDER;
      default -> throw new IllegalArgumentException("Unexpected topic: " + topic + "!");
    }

    context().forward(record.withValue(new TimestampWithStatus(record.timestamp(), status)));
  }
}

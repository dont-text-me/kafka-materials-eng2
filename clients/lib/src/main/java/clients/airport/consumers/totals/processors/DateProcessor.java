package clients.airport.consumers.totals.processors;

import clients.airport.AirportProducer;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;

public class DateProcessor
    extends ContextualProcessor<
        Integer, AirportProducer.TerminalInfo, String, AirportProducer.TerminalInfo> {

  @Override
  public void process(Record<Integer, AirportProducer.TerminalInfo> record) {
    String recordDate =
        Instant.ofEpochMilli(record.timestamp())
            .atZone(ZoneOffset.UTC)
            .format(DateTimeFormatter.BASIC_ISO_DATE);
    AirportProducer.TerminalInfo recordValue = record.value();
    context().forward(record.withKey(recordDate).withValue(recordValue));
  }
}

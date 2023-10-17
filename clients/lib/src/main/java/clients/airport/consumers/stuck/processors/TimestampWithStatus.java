package clients.airport.consumers.stuck.processors;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public record TimestampWithStatus(Long timestamp, DeskStatus status) {

  /** Compares two instances of TimestampWithStatus using the {@code timestamp} field. */
  public static TimestampWithStatus max(TimestampWithStatus left, TimestampWithStatus right) {
    if (left.timestamp > right.timestamp) {
      return left;
    }
    return right;
  }

  /** Compares two instances of TimestampWithStatus using the {@code timestamp} field. */
  public static TimestampWithStatus min(TimestampWithStatus left, TimestampWithStatus right) {
    if (left.timestamp < right.timestamp) {
      return left;
    }
    return right;
  }

  public static class TAndSSerializer implements Serializer<TimestampWithStatus> {
    private final Gson gson = new GsonBuilder().create();

    @Override
    public byte[] serialize(String topic, TimestampWithStatus data) {
      return gson.toJson(data).getBytes();
    }
  }

  public static class TAndSDeserializer implements Deserializer<TimestampWithStatus> {
    private final Gson gson = new GsonBuilder().create();

    @Override
    public TimestampWithStatus deserialize(String topic, byte[] data) {
      return gson.fromJson(new String(data), TimestampWithStatus.class);
    }
  }

  public static class TimestampWithStatusSerde extends Serdes.WrapperSerde<TimestampWithStatus> {

    public TimestampWithStatusSerde() {
      super(new TAndSSerializer(), new TAndSDeserializer());
    }
  }
}

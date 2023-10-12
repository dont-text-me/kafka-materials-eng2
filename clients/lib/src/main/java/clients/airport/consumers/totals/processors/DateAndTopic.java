package clients.airport.consumers.totals.processors;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public record DateAndTopic(String topic, String date) {

  public static class DAndTSerializer implements Serializer<DateAndTopic> {
    private final Gson gson = new GsonBuilder().create();

    @Override
    public byte[] serialize(String topic, DateAndTopic data) {
      return gson.toJson(data).getBytes();
    }
  }

  public static class DAndTDeserializer implements Deserializer<DateAndTopic> {
    private final Gson gson = new GsonBuilder().create();

    @Override
    public DateAndTopic deserialize(String topic, byte[] data) {
      return gson.fromJson(new String(data), DateAndTopic.class);
    }
  }

  public static class DateAndTopicSerde extends Serdes.WrapperSerde<DateAndTopic> {
    public DateAndTopicSerde() {
      super(new DAndTSerializer(), new DAndTDeserializer());
    }
  }
}

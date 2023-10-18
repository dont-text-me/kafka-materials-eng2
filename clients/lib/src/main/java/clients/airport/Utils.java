package clients.airport;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public class Utils {
  public static Integer getTerminalArea(Integer terminalId) {
    return terminalId / 100;
  }

  public static class GsonSerializer<T> implements Serializer<T> {
    private final Gson gson = new GsonBuilder().create();

    @Override
    public byte[] serialize(String topic, T data) {
      return gson.toJson(data).getBytes();
    }
  }

  public static class GsonDeserializer<T> implements Deserializer<T> {
    private final Gson gson = new GsonBuilder().create();
    private final Class<T> type;

    public GsonDeserializer(Class<T> type) {
      this.type = type;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
      return gson.fromJson(new String(data), this.type);
    }
  }

  public static class GsonSerde<T> extends Serdes.WrapperSerde<T> {

    public GsonSerde(Class<T> type) {
      super(new GsonSerializer<>(), new GsonDeserializer<>(type));
    }
  }
}

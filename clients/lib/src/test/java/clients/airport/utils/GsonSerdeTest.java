package clients.airport.utils;

import static org.junit.jupiter.api.Assertions.*;

import clients.airport.Utils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.jupiter.api.Test;

public class GsonSerdeTest {

  private enum TestEnum {
    VALUE,
    OTHER_VALUE,
    THIRD_VALUE,
  }

  private record TestData(Integer integerField, String stringField, TestEnum enumField) {}

  private final Gson gson = new GsonBuilder().create();

  private static final String TOPIC_PLACEHOLDER = "my-topic";

  @Test
  public void canSeriaize() {
    TestData testData = new TestData(123, "Hello world", TestEnum.OTHER_VALUE);
    Utils.GsonSerializer<TestData> sut = new Utils.GsonSerializer<>();

    byte[] serialized = sut.serialize(TOPIC_PLACEHOLDER, testData);

    TestData reconstructed = gson.fromJson(new String(serialized), TestData.class);

    assertEquals(testData, reconstructed);
  }

  @Test
  public void canDeserialize() {
    TestData expectedValue = new TestData(123, "Hello world", TestEnum.VALUE);
    byte[] testData = gson.toJson(expectedValue).getBytes();
    Utils.GsonDeserializer<TestData> sut = new Utils.GsonDeserializer<>(TestData.class);

    TestData reconstructed = sut.deserialize(TOPIC_PLACEHOLDER, testData);

    assertEquals(reconstructed, expectedValue);
  }
}

package clients.airport;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes.WrapperSerde;
import org.apache.kafka.common.serialization.Serializer;

public class AirportProducer extends AirportSimulator implements AutoCloseable {

  public static final String TOPIC_CANCELLED = "selfservice-cancelled";
  public static final String TOPIC_CHECKIN = "selfservice-checkin";
  public static final String TOPIC_COMPLETED = "selfservice-completed";
  public static final String TOPIC_LOWPAPER = "selfservice-lowpaper";
  public static final String TOPIC_OUTOFORDER = "selfservice-outoforder";
  public static final String TOPIC_STATUS = "selfservice-status";
  public static final String BOOTSTRAP_SERVERS = "localhost:9094,localhost:9095,localhost:9096";

  private static final Map<AirportSimulator.EventType, String> EVENT_TYPE_TO_TOPIC =
      new HashMap<>();

  static {
    EVENT_TYPE_TO_TOPIC.put(EventType.CANCELLED, TOPIC_CANCELLED);
    EVENT_TYPE_TO_TOPIC.put(EventType.CHECK_IN, TOPIC_CHECKIN);
    EVENT_TYPE_TO_TOPIC.put(EventType.COMPLETED, TOPIC_COMPLETED);
    EVENT_TYPE_TO_TOPIC.put(EventType.LOW_PAPER, TOPIC_LOWPAPER);
    EVENT_TYPE_TO_TOPIC.put(EventType.OUT_OF_ORDER, TOPIC_OUTOFORDER);
    EVENT_TYPE_TO_TOPIC.put(EventType.STATUS, TOPIC_STATUS);
  }

  public static class TerminalInfo {
    public boolean stuck;
    public int paperLeft;
  }

  public static class TerminalInfoSerializer implements Serializer<TerminalInfo> {
    @Override
    public byte[] serialize(String topic, TerminalInfo data) {
      ByteBuffer buf = ByteBuffer.allocate(1 + 4);
      buf.put((byte) (data.stuck ? 1 : 0));
      buf.putInt(data.paperLeft);
      return buf.array();
    }
  }

  public static class TerminalInfoDeserializer implements Deserializer<TerminalInfo> {
    @Override
    public TerminalInfo deserialize(String topic, byte[] data) {
      ByteBuffer buf = ByteBuffer.wrap(data);
      TerminalInfo info = new TerminalInfo();
      info.stuck = buf.get() == 1;
      info.paperLeft = buf.getInt();
      return info;
    }
  }

  public static class TerminalInfoSerde extends WrapperSerde<TerminalInfo> {
    public TerminalInfoSerde() {
      super(new TerminalInfoSerializer(), new TerminalInfoDeserializer());
    }
  }

  private KafkaProducer<Integer, TerminalInfo> producer;

  public AirportProducer(int nTerminals, Random rnd) {
    super(nTerminals, rnd);
  }

  @Override
  protected void fireEvent(EventType type, Terminal t) {
    super.fireEvent(type, t);

    if (producer == null) {
      Properties props = new Properties();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

      // Uncomment to use per-area partitioning
      // props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,
      // AreaPartitioner.class.getCanonicalName());

      producer = new KafkaProducer<>(props, new IntegerSerializer(), new TerminalInfoSerializer());
    }

    String topicName = EVENT_TYPE_TO_TOPIC.get(type);
    if (topicName != null) {
      TerminalInfo tInfo = new TerminalInfo();
      tInfo.stuck = t.isStuck();
      tInfo.paperLeft = t.getPaperLeft();

      producer.send(new ProducerRecord<>(topicName, t.getId(), tInfo));
    }
  }

  @Override
  public void close() {
    if (producer != null) {
      producer.close();
      producer = null;
    }
  }

  public static void main(String[] args) throws InterruptedException {
    try (AirportProducer producer = new AirportProducer(500, new Random(1))) {
      // Run at 10X speed
      producer.setSpeedupFactor(10);

      // Simulate 2 minutes's worth of events
      producer.runFor(Duration.ofMinutes(2));

      // Report how many actually broken-down machines we have, as a reference value
      final long crashedCount = producer.getTerminals().stream().filter(e -> e.isCrashed()).count();
      System.out.printf("%d terminals crashed during this simulation%n", crashedCount);
    }
  }
}

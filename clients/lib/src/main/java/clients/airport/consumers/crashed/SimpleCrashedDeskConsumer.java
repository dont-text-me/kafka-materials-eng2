package clients.airport.consumers.crashed;

import clients.airport.consumers.AbstractInteractiveShutdownConsumer;
import clients.messages.MessageProducer;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/** Consumer which will print out terminals that haven't a STATUS event in a while. */
public class SimpleCrashedDeskConsumer extends AbstractInteractiveShutdownConsumer {

  public void run() {
    Properties props = new Properties();
    props.put("bootstrap.servers", MessageProducer.BOOTSTRAP_SERVERS);
    props.put("group.id", "crashed-desks-simple");

    // Kafka will auto-commit every 5s based on the last poll() call
    props.put("enable.auto.commit", "true");

    Map<Integer, Instant> lastHeartbeat = new TreeMap<>();

    // TODO: exercise
  }

  public static void main(String[] args) {
    new SimpleCrashedDeskConsumer().runUntilEnterIsPressed(System.in);
  }
}

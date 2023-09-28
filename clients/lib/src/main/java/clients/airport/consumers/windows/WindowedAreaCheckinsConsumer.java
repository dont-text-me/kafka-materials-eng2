package clients.airport.consumers.windows;

import clients.airport.consumers.AbstractInteractiveShutdownConsumer;
import clients.messages.MessageProducer;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Computes windowed checkin counts over each general area of the airport (defined as the hundreds
 * digit of the terminal ID, e.g. terminal 403 is in area 4).
 *
 * <p>The first version compute counts correctly if we use more than one consumer in the group, and
 * it will forget events if we rebalance. We will fix these issues later on.
 */
public class WindowedAreaCheckinsConsumer extends AbstractInteractiveShutdownConsumer {

  private Duration windowSize = Duration.ofSeconds(30);

  public void run() {
    Properties props = new Properties();
    props.put("bootstrap.servers", MessageProducer.BOOTSTRAP_SERVERS);
    props.put("group.id", "windowed-area-stats");
    props.put("enable.auto.commit", "true");

    Map<Integer, TimestampSlidingWindow> windowCheckinsByArea = new HashMap<>();

    // TODO: exercise (check the TimestampSlidingWindow tests for examples of how to use it)
  }

  public static void main(String[] args) {
    new WindowedAreaCheckinsConsumer().runUntilEnterIsPressed(System.in);
  }
}

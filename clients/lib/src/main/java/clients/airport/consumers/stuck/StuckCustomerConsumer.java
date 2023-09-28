package clients.airport.consumers.stuck;

import clients.airport.consumers.AbstractInteractiveShutdownConsumer;
import clients.messages.MessageProducer;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Detects started checkins which get stuck in the middle due to an OUT_OF_ORDER event, and raises
 * them as events.
 */
public class StuckCustomerConsumer extends AbstractInteractiveShutdownConsumer {

  private static final String TOPIC_STUCK_CUSTOMERS = "selfservice-stuck-customers";

  public void run() {
    Properties props = new Properties();
    props.put("bootstrap.servers", MessageProducer.BOOTSTRAP_SERVERS);
    props.put("group.id", "stuck-customers-simple");
    props.put("enable.auto.commit", "true");

    Set<Integer> startedCheckins = new HashSet<>();

    // TODO: exercise
  }

  public static void main(String[] args) {
    new StuckCustomerConsumer().runUntilEnterIsPressed(System.in);
  }
}

package clients.messages;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/** Example producer which prints messages on screen. */
public class MessageProducer {

  public static final String BOOTSTRAP_SERVERS = "localhost:9094";
  public static final String TOPIC_NAME = "pingpong";

  public void run() {
    Properties props = new Properties();
    props.put("bootstrap.servers", BOOTSTRAP_SERVERS);

    try (Producer<String, String> producer =
            new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
        BufferedReader bR =
            new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
      System.out.print("Enter your name: ");
      String name = bR.readLine();

      while (true) {
        System.out.print("Enter a message, or write 'exit' to leave: ");
        String line = bR.readLine();
        if (line == null || "exit".equals(line)) {
          break;
        } else {
          producer.send(new ProducerRecord<>(TOPIC_NAME, name, line));
          System.out.println("Event sent!");
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    new MessageProducer().run();
  }
}

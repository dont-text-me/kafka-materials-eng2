package clients.airport.consumers;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;

import clients.airport.AirportProducer;
import clients.airport.AirportProducer.TerminalInfo;
import clients.airport.AirportProducer.TerminalInfoDeserializer;
import clients.messages.MessageProducer;

/**
 * Simple debugging-oriented consumer which just prints status events as they come.
 */
public class StatusPrinterConsumer extends AbstractInteractiveShutdownConsumer {

	@Override
	public void run() {
		Properties props = new Properties();
		props.put("bootstrap.servers", MessageProducer.BOOTSTRAP_SERVERS);
		props.put("group.id", "status-printers");
		props.put("enable.auto.commit", "true");

		try (KafkaConsumer<Integer, TerminalInfo> consumer =
					 new KafkaConsumer<>(
							 props,
							 new IntegerDeserializer(),
							 new TerminalInfoDeserializer()
					 )
		) {
			consumer.subscribe(Collections.singleton(AirportProducer.TOPIC_STATUS));

			while (!done) {
				ConsumerRecords<Integer, TerminalInfo> records = consumer.poll(Duration.ofSeconds(1));
				for (ConsumerRecord<Integer, TerminalInfo> record : records) {
					System.out.printf("Status event from %s at %s%n", record.key(), record.value());
				}
			}
		}
	}

	/**
	 * Runs the consumer until the user presses Enter. This works around a limitation
	 * in Eclipse that Ctrl+C cannot be sent from the Console view. Gradle's JavaExec
	 * task has a similar limitation.
	 */
	public static void main(String[] args) {
		new StatusPrinterConsumer().runUntilEnterIsPressed(System.in);
	}

}

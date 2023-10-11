package clients.airport;

import static clients.airport.Utils.getTerminalArea;

import java.util.Map;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.internals.BuiltInPartitioner;
import org.apache.kafka.common.Cluster;

/**
 * Alternative partitioner which partitions messages whose keys are check-in desk IDs by their
 * airport area (the hundreds digit).
 */
public class AreaPartitioner implements Partitioner {

  @Override
  public void configure(Map<String, ?> configs) {
    // nothing to do
  }

  @Override
  public int partition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    if (key instanceof Integer) {
      return getTerminalArea((Integer) key);
    }

    // Fall back to built-in partitioning if key is not an integer
    return BuiltInPartitioner.partitionForKey(keyBytes, cluster.partitionCountForTopic(topic));
  }

  @Override
  public void close() {
    // nothing to do
  }
}

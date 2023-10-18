package clients.airport.consumers.stuck.processors;

public record TimestampWithStatus(Long timestamp, DeskStatus status) {

  /** Compares two instances of TimestampWithStatus using the {@code timestamp} field. */
  public static TimestampWithStatus max(TimestampWithStatus left, TimestampWithStatus right) {
    if (left.timestamp > right.timestamp) {
      return left;
    }
    return right;
  }

  /** Compares two instances of TimestampWithStatus using the {@code timestamp} field. */
  public static TimestampWithStatus min(TimestampWithStatus left, TimestampWithStatus right) {
    if (left.timestamp < right.timestamp) {
      return left;
    }
    return right;
  }
}

package clients.airport.consumers.windows;

import java.time.Instant;
import java.util.NavigableSet;
import java.util.TreeSet;

/**
 * Utility class to count events within a sliding window. Use it by repeatedly calling {@link
 * #add(Instant)} to add events, and then get the count for a given window with {@link
 * #windowCount(Instant, Instant)} (which will also remove old events from memory).
 *
 * @see TimestampSlidingWindowTest for examples of use.
 */
public class TimestampSlidingWindow {
  private final NavigableSet<Instant> events = new TreeSet<>();

  public void add(Instant instant) {
    events.add(instant);
  }

  public int windowCount(Instant windowStart, Instant windowEnd) {
    while (!events.isEmpty()) {
      Instant earliest = events.first();
      if (earliest.isBefore(windowStart)) {
        events.remove(earliest);
      } else {
        break;
      }
    }

    int count = 0;
    for (Instant event : events) {
      if (event.isAfter(windowEnd)) {
        break;
      } else {
        ++count;
      }
    }
    return count;
  }
}

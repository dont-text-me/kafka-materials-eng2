package clients.airport.consumers.windows;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TimestampSlidingWindowTest {

  private TimestampSlidingWindow window;

  @BeforeEach
  public void setUp() {
    window = new TimestampSlidingWindow();
  }

  @Test
  public void empty() {
    assertEquals(0, window.windowCount(Instant.ofEpochMilli(0), Instant.now()));
  }

  @Test
  public void rangesAreClosed() {
    for (int i = 1; i < 10; i++) {
      window.add(Instant.ofEpochMilli(i));
    }

    // Ranges are closed: [start, end]
    assertEquals(4, window.windowCount(Instant.ofEpochMilli(1), Instant.ofEpochMilli(4)));
  }

  @Test
  public void automaticDiscardingOfOldEvents() {
    for (int i = 1; i < 10; i++) {
      window.add(Instant.ofEpochMilli(i));
    }

    // This will count all three events and not discard anything yet
    assertEquals(3, window.windowCount(Instant.ofEpochMilli(1), Instant.ofEpochMilli(3)));

    // This will discard the event before the start of the window
    assertEquals(2, window.windowCount(Instant.ofEpochMilli(2), Instant.ofEpochMilli(3)));

    // This should now report only instants 2 and 3
    assertEquals(2, window.windowCount(Instant.ofEpochMilli(1), Instant.ofEpochMilli(3)));
  }
}

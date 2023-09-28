package clients.airport.consumers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Base class for consumers that shut down when Enter is pressed. This is a workaround for a
 * limitation in how Ctrl+C is handled by the Eclipse Console view and the Gradle JavaExec task,
 * where the SIGINT signal is not sent to the Java process, so the automated shutdown is not
 * triggered.
 */
public abstract class AbstractInteractiveShutdownConsumer implements Runnable {

  /**
   * Flag used for easy shutdown by pressing Enter in the console view.
   *
   * <p>Uses 'volatile' to ensure threads always read the latest version of it, and not a stale
   * locally-cached version for their thread.
   */
  protected volatile boolean done;

  public void shutdown() {
    done = true;
  }

  public void runUntilEnterIsPressed(InputStream is) {
    // Resets the 'done' flag
    done = false;

    // Runs the consumer on its own thread
    Thread tConsumer = new Thread(this, getClass().getSimpleName());
    tConsumer.start();

    // Waits until the user presses Enter
    try (BufferedReader r = new BufferedReader(new InputStreamReader(is))) {
      r.readLine();
    } catch (IOException e) {
      e.printStackTrace();
    }

    // Asks the consumer to cleanly shut down, and waits for it
    shutdown();
    try {
      tConsumer.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}

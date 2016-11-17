package nakadi.examples.certs;

import java.util.concurrent.TimeUnit;
import nakadi.ExponentialRetry;
import nakadi.NakadiClient;

public class CertsMain {

  public static void main(String[] args) {

    /*
     change base uri as needed to one with a self-signed/other cert
      */
    String baseURI = "http://localhost:" + 9080;

    /*
     Load the certs from the classpath, you can also a file:/// uri here. The purpose of
     this feature is stop people turning off ssl because it's "just" development/staging
     or some such. Turning off ssl isn't an option for the client.
     The logs will be tagged with "[security_support]" during the loading step
     */
    NakadiClient client = NakadiClient.newBuilder()
        .baseURI(baseURI)
        .certificatePath("classpath:certs")
        .build();

    // run a healthcheck with a few retries to see if the server's up
    client.resources().health().retryPolicy(buildExponentialRetry(3)).healthcheckThrowing();

  }

  private static ExponentialRetry buildExponentialRetry(int maxAttempts) {
    return ExponentialRetry.newBuilder()
        .initialInterval(1000, TimeUnit.MILLISECONDS)
        .maxAttempts(maxAttempts)
        .maxInterval(3000, TimeUnit.MILLISECONDS)
        .build();
  }
}

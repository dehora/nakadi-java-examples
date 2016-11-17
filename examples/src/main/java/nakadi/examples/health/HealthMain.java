package nakadi.examples.health;

import java.util.concurrent.TimeUnit;
import nakadi.ExponentialRetry;
import nakadi.HealthCheckResource;
import nakadi.NakadiClient;
import nakadi.Response;


class HealthMain {

  public static void main(String[] args) throws Exception {

    // change base uri as needed
    String baseURI = "http://localhost:" + 9080;

    NakadiClient client = NakadiClient.newBuilder().baseURI(baseURI).build();

    HealthCheckResource health = client.resources().health();

    System.out.println("Healthcheck non-throwing: Result");
    System.out.println("-----------------------------------------");
    Response healthcheck = health.healthcheck();
    System.out.println(healthcheck.statusCode() + " " + healthcheck.reason());
    System.out.println(healthcheck.headers());
    System.out.println(healthcheck.responseBody().asString());
    System.out.println("\n");

    System.out.println("Healthcheck retrying and throwing: Result");
    System.out.println("-----------------------------------------");
    ExponentialRetry retry = buildExponentialRetry(5);
    healthcheck = health
        .retryPolicy(retry)
        .healthcheckThrowing();
    System.out.println("retry details: " + retry);
    System.out.println(healthcheck.statusCode() + " " + healthcheck.reason());
    System.out.println(healthcheck.headers());
    System.out.println(healthcheck.responseBody().asString());
    // wipe our retry
    health.retryPolicy(null);
    System.out.println("\n");


    System.out.println("Healthcheck throwing: Result");
    System.out.println("-----------------------------------------");
    healthcheck = health.healthcheckThrowing();
    System.out.println(healthcheck.statusCode() + " " + healthcheck.reason());
    System.out.println(healthcheck.headers());
    System.out.println(healthcheck.responseBody().asString());
    System.out.println("\n");

  }

  private static ExponentialRetry buildExponentialRetry(int maxAttempts) {
    return ExponentialRetry.newBuilder()
        .initialInterval(1000, TimeUnit.MILLISECONDS)
        .maxAttempts(maxAttempts)
        .maxInterval(8000, TimeUnit.MILLISECONDS)
        .build();
  }

}

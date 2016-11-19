package nakadi.examples.eventtypes;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import nakadi.EventType;
import nakadi.EventTypeCollection;
import nakadi.EventTypeOptions;
import nakadi.EventTypeResource;
import nakadi.EventTypeSchema;
import nakadi.ExponentialRetry;
import nakadi.NakadiClient;
import nakadi.Response;

public class EventTypeMain {

  public static void main(String[] args) {

    // change base uri as needed
    String baseURI = "http://localhost:" + 9080;

    NakadiClient client = NakadiClient.newBuilder().baseURI(baseURI).build();

    // run a healthcheck with a few retries to see if the server's up
    client.resources().health().retryPolicy(buildExponentialRetry(3)).healthcheckThrowing();

    createDataChangeEvent(client);
  }

  private static void createDataChangeEvent(NakadiClient client) {

    EventTypeResource eventTypes = client.resources().eventTypes();

    String eventTypeName = "nakadi-example-" + new Random().nextInt(999);

    String owningApplication = "weyland";
    EventTypeOptions options = new EventTypeOptions().retentionTime(3, TimeUnit.DAYS);

    EventType et = new EventType()
        .name(eventTypeName)
        .category(EventType.Category.data)
        .owningApplication(owningApplication)
        .options(options)
        .partitionStrategy(EventType.PARTITION_HASH)
        .enrichmentStrategies(EventType.ENRICHMENT_METADATA)
        .partitionKeyFields("id")
        .schema(new EventTypeSchema().schema(
            "{ \"properties\": { \"id\": { \"type\": \"string\" } } }"));

    Response response = eventTypes.create(et);
    System.out.println("Create Event Type: Result");
    System.out.println("-----------------------------------------");
    System.out.println("creating: "+ et.name());
    System.out.println(response.statusCode() + " " + response.reason());
    System.out.println(response.headers());
    System.out.println(response.responseBody().asString());

    System.out.println("Refetching Created Event Type: Result");
    System.out.println("-----------------------------------------");
    System.out.println("finding: "+ eventTypeName);
    EventType eventType = eventTypes.findByName(eventTypeName);
    System.out.println(eventType);

    System.out.println("Event Type List: Result");
    System.out.println("-----------------------------------------");
    EventTypeCollection list = eventTypes.list();
    list.iterable().forEach(System.out::println);

    System.out.println("Deleting Created Event Type: Result");
    System.out.println("-----------------------------------------");
    Response delete = eventTypes.delete(eventTypeName);
    System.out.println("deleting: "+ eventTypeName);
    System.out.println(delete.statusCode() + " " + delete.reason());
    System.out.println(delete.headers());
    System.out.println(delete.responseBody().asString());


  }

  private static ExponentialRetry buildExponentialRetry(int maxAttempts) {
    return ExponentialRetry.newBuilder()
        .initialInterval(1000, TimeUnit.MILLISECONDS)
        .maxAttempts(maxAttempts)
        .maxInterval(3000, TimeUnit.MILLISECONDS)
        .build();
  }
}

package nakadi.examples.events;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import nakadi.DataChangeEvent;
import nakadi.EventMetadata;
import nakadi.EventResource;
import nakadi.EventType;
import nakadi.EventTypeOptions;
import nakadi.EventTypeResource;
import nakadi.EventTypeSchema;
import nakadi.ExponentialRetry;
import nakadi.NakadiClient;
import nakadi.NotFoundException;
import nakadi.Response;
import nakadi.RetryPolicy;

public class EventsMain {

  public static void main(String[] args) {

    // change base uri as needed
    String baseURI = "http://localhost:" + 9080;

    /*
     minimal client; this is all we need for posting to a local dev server.
     you'll need a token provider to post to a server configured to honor auth
      */
    NakadiClient client = NakadiClient.newBuilder().baseURI(baseURI).build();

    // run a healthcheck with a few retries to see if the server's up
    client.resources().health().retryPolicy(buildExponentialRetry(3)).healthcheckThrowing();

    // check and create the event type if it's not there already
    createDataChangeEvent(client);

    sendOneEvent(client);
    sendSomeEvents(client);
    sendOneEventRetrying(client);
  }

  private static void sendOneEvent(NakadiClient client) {
    EventResource resource = client.resources().events();

    String eventTypeName = "priority-requisition";

    // eid and occurredAt are set automatically if not supplied, this by way of example
    EventMetadata em = new EventMetadata()
        .eid(UUID.randomUUID().toString())
        .occurredAt(OffsetDateTime.now());

    PriorityRequisition pr = new PriorityRequisition("22");

    /*
    wrap our domain object in a DataChangeEvent. The send() method will take any generic
    object, this just shows an example using the inbuilt categories.
     */
    DataChangeEvent<PriorityRequisition> event = new DataChangeEvent<PriorityRequisition>()
        .metadata(em)
        .op(DataChangeEvent.Op.C)
        .dataType(eventTypeName)
        .data(pr);

    Response response = resource.send(eventTypeName, event);
    System.out.println("\nSend 1 Event: Result ==========================");
    System.out.println(response.statusCode() + " " + response.reason());
    System.out.println(response.headers());
    System.out.println(response.responseBody().asString());
  }

  private static void sendSomeEvents(NakadiClient client) {

    EventResource resource = client.resources().events();
    String eventTypeName = "priority-requisition";
    ArrayList<DataChangeEvent<PriorityRequisition>> events = new ArrayList<>();

    for (int i = 0; i < 8; i++) {
      DataChangeEvent<PriorityRequisition> event = new DataChangeEvent<PriorityRequisition>()
          .metadata(new EventMetadata())
          .op(DataChangeEvent.Op.C)
          .dataType(eventTypeName)
          .data(new PriorityRequisition("" + i));

      events.add(event);
    }

    Response response = resource.send(eventTypeName, events);
    System.out.println("\nSend Some Events: Result ==========================");
    System.out.println(response.statusCode() + " " + response.reason());
    System.out.println(response.headers());
    System.out.println(response.responseBody().asString());
  }

  private static void sendOneEventRetrying(NakadiClient client) {
    EventResource resource = client.resources().events();

    String eventTypeName = "priority-requisition";

    PriorityRequisition pr = new PriorityRequisition("23");

    DataChangeEvent<PriorityRequisition> event = new DataChangeEvent<PriorityRequisition>()
        .metadata(new EventMetadata())
        .op(DataChangeEvent.Op.C)
        .dataType(eventTypeName)
        .data(pr);

    /*
     you can set a retry, but fair warning, there's zero assurances about ordering
     of submitted events if the retry kicks in.
      */
    RetryPolicy retry = buildExponentialRetry(4);

    Response response = resource.
        retryPolicy(retry)
        .send(eventTypeName, event);

    System.out.println("\nSend 1 Event Retrying: Result ==========================");
    System.out.println("retry details: " + retry);
    System.out.println(response.statusCode() + " " + response.reason());
    System.out.println(response.headers());
    System.out.println(response.responseBody().asString());
  }

  private static void createDataChangeEvent(NakadiClient client) {

    EventTypeResource eventTypes = client.resources().eventTypes();

    String eventTypeName = "priority-requisition";

    try {
      client.resources().eventTypes().findByName(eventTypeName);
      System.out.println("\nFound our Event Type ==========================");
      EventType eventType = client.resources().eventTypes().findByName(eventTypeName);
      System.out.println(eventType);
      return;
    } catch (NotFoundException ignored) {
    }

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
    System.out.println("\nCreate Event Type Result ==========================");
    System.out.println(response.statusCode() + " " + response.reason());
    System.out.println(response.headers());
    System.out.println(response.responseBody().asString());

    System.out.println("\nRefetching Created Event Type ==========================");
    EventType eventType = client.resources().eventTypes().findByName(eventTypeName);
    System.out.println(eventType);
  }

  private static ExponentialRetry buildExponentialRetry(int maxAttempts) {
    return ExponentialRetry.newBuilder()
        .initialInterval(1000, TimeUnit.MILLISECONDS)
        .maxAttempts(maxAttempts)
        .maxInterval(3000, TimeUnit.MILLISECONDS)
        .build();
  }
}

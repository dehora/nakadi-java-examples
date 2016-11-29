package nakadi.examples.streams;

import java.util.ArrayList;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import nakadi.BusinessEventMapped;
import nakadi.EventMetadata;
import nakadi.EventResource;
import nakadi.EventType;
import nakadi.EventTypeOptions;
import nakadi.EventTypeResource;
import nakadi.EventTypeSchema;
import nakadi.NakadiClient;
import nakadi.Response;
import nakadi.StreamConfiguration;
import nakadi.StreamProcessor;
import nakadi.Subscription;
import nakadi.SubscriptionResource;
import nakadi.examples.events.PriorityRequisition;

/**
 * See {@link SubscriptionStreamMain} for an annotated example of setting up a
 * subscription and stream. This focuses on the {@link BusinessEventMapped}.
 */
public class BusinessSubscriptionStreamMain {

  public static void main(String[] args) throws Exception {
    String baseURI = "http://localhost:" + 9080;
    NakadiClient client = NakadiClient.newBuilder()
        .baseURI(baseURI)
        .build();

    String eventTypeName = "nakadi-java-examples-BusinessSubscriptionStreamMain-subscription-"
        + new Random().nextInt(999);

    EventType et = getOrCreateBusinessEventEvent(eventTypeName, client);

    sendSomeEvents(et.name(), client);

    Subscription subscription = createSubscription(et.name(), client);

    StreamConfiguration sc = new StreamConfiguration()
        .batchLimit(5)
        .batchFlushTimeout(3, TimeUnit.SECONDS)
        .subscriptionId(subscription.id())
        .maxUncommittedEvents(1000L)
        .maxRetryDelay(16, TimeUnit.SECONDS);

    StreamProcessor processor = client.resources()
        .streamBuilder(sc)
        .streamObserverFactory(new BusinessEventStreamObserverProvider())
        .build();

    processor.start();
    System.out.println("ok, streams running");
    Thread.sleep(60_000L);
    System.out.println("shutting down");
    System.exit(0);
  }

  private static void sendSomeEvents(String eventTypeName, NakadiClient client) {

    EventResource events = client.resources().events();

    int batchSize = 5;
    int batchCount = 5;
    ArrayList<BusinessEventMapped<PriorityRequisition>> eventList = new ArrayList<>();

    for (int b = 0; b < batchCount; b++) {
      for (int i = 0; i < batchSize; i++) {
        final BusinessEventMapped<PriorityRequisition> event =
            new BusinessEventMapped<PriorityRequisition>()
                .metadata(new EventMetadata())
                .data(new PriorityRequisition(b + "-" + i));
        eventList.add(event);
      }

      Response response = events.send(eventTypeName, eventList);
      System.out.println("Send a batch of "
          + batchSize
          + ": Result "
          + response.statusCode()
          + " "
          + response.reason());
    }
  }

  private static EventType getOrCreateBusinessEventEvent(String eventTypeName, NakadiClient client) {

    EventTypeResource eventTypes = client.resources().eventTypes();

    Optional<EventType> eventTypeMaybe = eventTypes.tryFindByName(eventTypeName);
    if (eventTypeMaybe.isPresent()) {
      return eventTypeMaybe.get();
    }

    String owningApplication = "weyland";
    EventTypeOptions options = new EventTypeOptions().retentionTime(3, TimeUnit.DAYS);

    EventType et = new EventType()
        .name(eventTypeName)
        .category(EventType.Category.business)
        .owningApplication(owningApplication)
        .options(options)
        .partitionStrategy(EventType.PARTITION_HASH)
        .enrichmentStrategies(EventType.ENRICHMENT_METADATA)
        .partitionKeyFields("id")
        .schema(new EventTypeSchema().schema(
            "{ \"properties\": { \"id\": { \"type\": \"string\" } } }"));

    eventTypes.create(et);
    EventType eventType = eventTypes.findByName(eventTypeName);
    System.out.println(eventType);

    return eventType;
  }

  private static Subscription createSubscription(String eventTypeName, NakadiClient client) {

    SubscriptionResource subscriptions = client.resources().subscriptions();

    String consumerGroup =
        "nakadi-java-examples-BusinessEventSubscriptionStreamMain-cg-" + +new Random().nextInt(999);

    Subscription create = new Subscription()
        .consumerGroup(consumerGroup)
        .eventType(eventTypeName)
        .readFrom("begin") // read from the oldest event
        .owningApplication("shaper");

    Subscription response = subscriptions.create(create);
    return subscriptions.find(response.id());
  }
}

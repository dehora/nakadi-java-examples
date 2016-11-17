package nakadi.examples.subscriptions;

import java.util.concurrent.TimeUnit;
import nakadi.EventType;
import nakadi.EventTypeOptions;
import nakadi.EventTypeResource;
import nakadi.EventTypeSchema;
import nakadi.NakadiClient;
import nakadi.NotFoundException;
import nakadi.QueryParams;
import nakadi.Response;
import nakadi.Subscription;
import nakadi.SubscriptionCollection;
import nakadi.SubscriptionCursorCollection;
import nakadi.SubscriptionEventTypeStatsCollection;
import nakadi.SubscriptionResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SubscriptionMain {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());

  public static void main(String[] args) throws Exception {

    // change base uri as needed
    String baseURI = "http://localhost:" + 9080;

    NakadiClient client = NakadiClient.newBuilder().baseURI(baseURI).build();

    String eventTypeName = "priority-requisition";

    // make sure we have an event type in place
    EventType et =  getOrCreateEventType(client, eventTypeName);

    SubscriptionResource subscriptions = client.resources().subscriptions();

    createSubscription(eventTypeName, subscriptions);

    subscriptions(client);
  }

  private static void createSubscription(String eventTypeName, SubscriptionResource subscriptions) {

    Subscription create = new Subscription()
        .consumerGroup("meidan-consumer-group-" + System.currentTimeMillis() / 10000)
        .eventType(eventTypeName)
        .readFrom("begin")
        .owningApplication("shaper");

    Subscription response = subscriptions.create(create);

    System.out.println("Subscription create: Result");
    System.out.println("-----------------------------------------");
    System.out.println(response);

    Subscription found = subscriptions.find(response.id());

    System.out.println("Subscription refind: Result");
    System.out.println("-----------------------------------------");
    System.out.println(found);
  }

  private static EventType getOrCreateEventType(NakadiClient client, String eventTypeName) {

    EventTypeResource eventTypes = client.resources().eventTypes();

    try {
      client.resources().eventTypes().findByName(eventTypeName);
      return client.resources().eventTypes().findByName(eventTypeName);
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
    System.out.println("Create Event Type Result");
    System.out.println("-----------------------------------------");
    System.out.println(response.statusCode() + " " + response.reason());
    System.out.println(response.headers());
    System.out.println(response.responseBody().asString());

    return client.resources().eventTypes().findByName(eventTypeName);
  }

  private static void subscriptions(NakadiClient client) {

    SubscriptionResource resource = client.resources().subscriptions();

    System.out.println("Subscription list: Result");
    System.out.println("-----------------------------------------");
    SubscriptionCollection list = resource.list();
    list.iterable().forEach(System.out::println);

    System.out.println("Subscription list with query: Result");
    System.out.println("-----------------------------------------");
    resource.list(new QueryParams().param("owning_application", "shaper"))
        .iterable()
        .forEach(System.out::println);

    System.out.println("Subscription find: Result");
    System.out.println("-----------------------------------------");
    Subscription first = list.items().get(0);
    Subscription found = resource.find(first.id());
    System.out.println(found);

    System.out.println("Subscription cursors: Result");
    System.out.println("-----------------------------------------");
    SubscriptionCursorCollection cursors = resource.cursors(found.id());
    System.out.println(found.id());
    cursors.iterable().forEach(System.out::println);

    System.out.println("Subscription stats: Result");
    System.out.println("-----------------------------------------");
    SubscriptionEventTypeStatsCollection stats = resource.stats(found.id());
    System.out.println(found.id());
    System.out.println(stats);
  }
}

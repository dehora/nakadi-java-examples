package nakadi.examples.streams;

import java.util.ArrayList;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import nakadi.DataChangeEvent;
import nakadi.EventMetadata;
import nakadi.EventResource;
import nakadi.EventType;
import nakadi.EventTypeOptions;
import nakadi.EventTypeResource;
import nakadi.EventTypeSchema;
import nakadi.LoggingStreamObserverProvider;
import nakadi.NakadiClient;
import nakadi.Response;
import nakadi.StreamConfiguration;
import nakadi.StreamProcessor;
import nakadi.Subscription;
import nakadi.SubscriptionResource;
import nakadi.examples.events.PriorityRequisition;

public class SubscriptionStreamMain {

  public static void main(String[] args) throws Exception {
    // change base uri as needed
    String baseURI = "http://localhost:" + 9080;
    NakadiClient client = NakadiClient.newBuilder()
        //.enableHttpLogging() // enable this if you want to see http traces
        .baseURI(baseURI)
        .build();

    String eventTypeName = "meidan-subscription-example-" + new Random().nextInt(999);
    Subscription subscription = setup(eventTypeName, client);
    sendSomeEvents(eventTypeName, client);

    /*
     See the javadoc for the full set of options
     */
    StreamConfiguration sc = new StreamConfiguration()
        .batchLimit(5)
        .batchFlushTimeout(3, TimeUnit.SECONDS)
        .subscriptionId(subscription.id())
        .maxUncommittedEvents(1000L)
        .maxRetryDelay(16, TimeUnit.SECONDS);

    /*
    build our processors. The observer is the inbuilt logger which just strings
    the events in the batches as they arrive.

    The offset observer is the inbuilt which commits cursors back to nakadi. If you want
    to see it processing in the logs, set the logback-test file to debug and look for
    "subscription_checkpoint" markers in the logs. Failing that you'll  see "keepalive"
    messages after the initial clump of events are consumed with an offset indicating the
    commits made it back to the server.
     */
    StreamProcessor processor = client.resources()
        .streamBuilder(sc)
        .streamObserverFactory(new LoggingStreamObserverProvider())
        .build();

    StreamProcessor processor1 = client.resources()
        .streamBuilder(sc)
        .streamObserverFactory(new LoggingStreamObserverProvider())
        .build();

    /*
    run our processors in the background. one of these will go into retry/backoff
    due to 409s as there's only one partition available to grab.
     */
    processor.start();
    processor1.start();

    /*
     block main for a bit to let the events be consumed and the retry/keepalive cycle run
     a few times, before exiting.
     */
    System.out.println("ok, streams running");
    Thread.sleep(60_000L);
    System.out.println("shutting down");
    System.exit(0);

    /*

    A few things on the connection handling:

    - if you configure streamLimit on StreamConfiguration, the processor will exit after
    seeing that many events instead of falling back to a listening mode.

    - once a client handling a partition goes away, the other running clients will compete
    to grab it once they wake up and retry. One of them will win and start streaming from
    the offset nakadi has stored for the subscription. Because they work by retrying every
    n seconds to take a partition and falling back, it's ok to run the client across your
    very large microservices cluster - even for just a few partitions , they
    won't hammer the server by default, so no special handling needed there. You can control
    the retry delay via maxRetryDelay on the StreamConfiguration, but the client won't accept
    a value here that's less that one second.

    - if the nakadi server isn't around on startup, the processors will go into a sleep
    retry cycle on an exponential backoff up to a max time.

    - if the nakadi server doesn't send a keepalive within the batchFlushTimeout plus a
    grace period, the client will disconnect and reconnect; this is done to handle
    half-open connections and other tcp/ip weirdness. it's pretty hard to emulate without
    faking out nakadi itself, but just letting you know it's there.

    - the client will handle some errors and go into a retry mode. others it will give up
    on.  You can see a bit more about that in the client's junit tests here: http://bit.ly/2f56klj
     */
  }

  private static void sendSomeEvents(String eventTypeName, NakadiClient client) {
  /*
   write batches events into the stream; our sub is set to consume from the oldest event
   so we'll see some events being consumed.
    */

    EventResource events = client.resources().events();

    int batchSize = 5;
    int batchCount = 5;
    ArrayList<DataChangeEvent<PriorityRequisition>> eventList = new ArrayList<>();

    for (int b = 0; b < batchCount; b++) {
      for (int i = 0; i < batchSize; i++) {
        final DataChangeEvent<PriorityRequisition> event =
            new DataChangeEvent<PriorityRequisition>()
                .metadata(new EventMetadata())
                .op(DataChangeEvent.Op.C)
                .dataType(eventTypeName)
                .data(new PriorityRequisition(b + "-" + i));
        eventList.add(event);
      }

      Response response = events.send(eventTypeName, eventList);
      System.out.println("Send a batch of "+batchSize+": Result " + response.statusCode() + " " + response.reason());
    }
  }

  public static Subscription setup(String eventTypeName, NakadiClient client) {
    EventType et = getOrCreateDataChangeEvent(eventTypeName, client);
    return createSubscription(et.name(), client);
  }

  private static EventType getOrCreateDataChangeEvent(String eventTypeName, NakadiClient client) {

    EventTypeResource eventTypes = client.resources().eventTypes();

    Optional<EventType> eventTypeMaybe = eventTypes.tryFindByName(eventTypeName);
    if(eventTypeMaybe.isPresent()) {
      return eventTypeMaybe.get();
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
    System.out.println("Create Event Type: Result");
    System.out.println("-----------------------------------------");
    System.out.println("creating: " + et.name());
    System.out.println(response.statusCode() + " " + response.reason());
    System.out.println(response.headers());
    System.out.println(response.responseBody().asString());

    System.out.println("Refetching Created Event Type: Result");
    System.out.println("-----------------------------------------");
    System.out.println("finding: " + eventTypeName);
    EventType eventType = eventTypes.findByName(eventTypeName);
    System.out.println(eventType);

    return eventType;
  }

  private static Subscription createSubscription(String eventTypeName, NakadiClient client) {

    SubscriptionResource subscriptions = client.resources().subscriptions();

    String consumerGroup =
        "meidan-subscription-example-consumer-group-" + +new Random().nextInt(999);

    Subscription create = new Subscription()
        .consumerGroup(consumerGroup)
        .eventType(eventTypeName)
        .readFrom("begin") // read from the oldest event
        .owningApplication("shaper");

    Subscription response = subscriptions.create(create);

    System.out.println("Subscription create: Result");
    System.out.println("-----------------------------------------");
    System.out.println("id:" + response.id());
    System.out.println("consumer group:" + response.consumerGroup());
    System.out.println("more:" + response);

    return subscriptions.find(response.id());
  }
}

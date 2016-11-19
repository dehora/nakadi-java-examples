package nakadi.examples.oauth;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import nakadi.EventType;
import nakadi.EventTypeResource;
import nakadi.ExponentialRetry;
import nakadi.NakadiClient;
import nakadi.token.zign.TokenProviderZign;

import static java.lang.System.out;

public class ZignTokenProviderMain {

  /*
  zign seems to require python3 called as python which can be fiddly to setup from
  intellij. if you're seeing exit_value of 1 in the logs for token fetches try the

       ./wgradle :examples:zignTokenProviderMain gradle

  command instead. Once it's running you'll see logs from "TokenProviderZign"
  periodically refresh; an exit_value=0 means the token was fetched.

   */
  public static void main(String[] args) {
    /*
    create a zign token provider and start it. This can run in the background and refresh
    on a timer using refreshEvery. The waitFor value says how long the background call should
    block for each token call in the refresh cycle before giving up.
     */
    TokenProviderZign zignTokenProvider = TokenProviderZign.newBuilder()
        .refreshEvery(60, TimeUnit.SECONDS)
        .waitFor(5, TimeUnit.SECONDS)
        .build();

    /*
    ztp will try and load the tokens first and then move to the background. That is,
    start will block initially on first load, so it's usually ok to do this and then
    give it to the client without worrying whether api calls will be made before tokens
    are fetched, unless there's an issue with the underlying zign call.
     */
    zignTokenProvider.start();
    /*
    normally, you'd let this run in the background, but for the example just load the
    tokens once and stop
     */
    zignTokenProvider.stop();

    /*
    make sure this service is enabled to work with tokens. also if you're calling
    a service with self-signed certs, you'll want those added to the jvm keystore
    or loaded in via certificatePath (see CertsMain for an example)
     */
    String baseURI = "http://localhost:" + 9080;

    NakadiClient client = NakadiClient.newBuilder()
        .baseURI(baseURI)
        .tokenProvider(zignTokenProvider)
        .build();

    EventTypeResource eventTypes = client.resources().eventTypes();

    eventTypes
        .retryPolicy(buildExponentialRetry(3))
        .list()
        .iterable()
        .forEach(et -> {
          out.println("event type: " + et.name());
          out.println(
              "--------------------------------------------------------------------------------------");
          out.println("owner: " + et.owningApplication());
          out.println("category: " + et.category());
          out.println("partition strategy: " + et.partitionStrategy());
          out.println("partition keys: " + et.partitionKeyFields());

          out.println(
              String.format("retention: %d (%d hours)", et.options().retentionTimeMillis(),
                  TimeUnit.MILLISECONDS.toHours(et.options().retentionTimeMillis())));
          out.println("enrichments: " + et.enrichmentStrategies());
          out.println("read scopes: " + et.readScopes());
          out.println("write scopes: " + et.writeScopes());
          out.println("schema follows:");
          out.println(unescapeSchema(client, et));
          out.println("\n");
        });
  }

  private static String unescapeSchema(NakadiClient client, EventType et) {
    try {
      String unescaped = et.schema().schema().replace("\\", "");
      return client.jsonSupport().toJson(client.jsonSupport().fromJson(unescaped, Map.class));
    } catch (Exception e) {
      out.println("error processing schema, will return raw " + e.getMessage());
      return et.schema().schema();
    }
  }

  private static ExponentialRetry buildExponentialRetry(int maxAttempts) {
    return ExponentialRetry.newBuilder()
        .initialInterval(1000, TimeUnit.MILLISECONDS)
        .maxAttempts(maxAttempts)
        .maxInterval(3000, TimeUnit.MILLISECONDS)
        .build();
  }
}

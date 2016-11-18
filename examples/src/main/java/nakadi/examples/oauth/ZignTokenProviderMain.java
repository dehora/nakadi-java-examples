package nakadi.examples.oauth;

import java.util.concurrent.TimeUnit;
import nakadi.BusinessEventMapped;
import nakadi.EventMetadata;
import nakadi.EventResource;
import nakadi.NakadiClient;
import nakadi.examples.events.PriorityRequisition;
import nakadi.token.zign.TokenProviderZign;

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
    create a zign token provider and start it. This will run in the background and refresh
    on a timer using refreshEvery. The waitFor value says how long the background call should
    block for each token call in the refresh cycle before giving up.
     */
    TokenProviderZign zignTokenProvider = TokenProviderZign.newBuilder()
        .refreshEvery(60, TimeUnit.SECONDS)
        .waitFor(5, TimeUnit.SECONDS)
        // default scopes added automatically
        .scopes("gordian-blade-scope")
        .build();

    /*
    ztp will try and load the tokens first and then move to the background. That is,
    start will block initially on first load, so it's usually ok to do this and then
    give it to the client without worrying whether api calls will be made before tokens
    are fetched, unless there's an issue with the underlying zign call.
     */
    zignTokenProvider.start();

    String baseURI = "http://localhost:" + 9080;

    NakadiClient client = NakadiClient.newBuilder()
        .baseURI(baseURI)
        .tokenProvider(zignTokenProvider)
        .build();

    BusinessEventMapped<PriorityRequisition> event = new BusinessEventMapped<PriorityRequisition>()
        .metadata(new EventMetadata())
        .data(new PriorityRequisition("22"));

    EventResource events = client.resources().events();

    /*
    use the default scope (in this case it's NAKADI_EVENT_STREAM_WRITE
     */
    events.send("priority-requisition-biz", event);

  }
}

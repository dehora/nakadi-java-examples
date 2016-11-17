package nakadi.examples.oauth;

import nakadi.BusinessEventMapped;
import nakadi.EventMetadata;
import nakadi.EventResource;
import nakadi.NakadiClient;
import nakadi.examples.events.PriorityRequisition;

public class OAuthScopesMain {

  public static void main(String[] args) {

    String baseURI = "http://localhost:" + 9080;

    NakadiClient client = NakadiClient.newBuilder()
        .baseURI(baseURI)
        /*
        Configure the client with a token provider. All scopes are sent to this TokenProvider
         per request to be resolved to tokens. Because it's per request, providers can refresh
         in the background.
         */
        .tokenProvider(new MyTokenProvider())
        .build();

    BusinessEventMapped<PriorityRequisition> event = new BusinessEventMapped<PriorityRequisition>()
        .metadata(new EventMetadata())
        .data(new PriorityRequisition("22"));

    EventResource events = client.resources().events();

    /*
    You can set the oauth scope on most requests using the scope() option.
    This allows for custom or tenant level scopes to be used (in the future).
    Otherwise the default scopes defined in the Nakadi API definition are used.
     */
    events
        .scope("gordian-blade-scope")
        .send("priority-requisition-biz", event);

  }
}

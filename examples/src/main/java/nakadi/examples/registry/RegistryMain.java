package nakadi.examples.registry;

import nakadi.EnrichmentStrategyCollection;
import nakadi.NakadiClient;
import nakadi.RegistryResource;
import nakadi.ValidationStrategyCollection;

class RegistryMain {

  public static void main(String[] args) throws Exception {

    // change base uri as needed
    String baseURI = "http://localhost:" + 9080;

    NakadiClient client = NakadiClient.newBuilder().baseURI(baseURI).build();

    RegistryResource registry = client.resources().registry();

    System.out.println("Enrichments: Result");
    System.out.println("-----------------------------------------");
    EnrichmentStrategyCollection enrichmentStrategyCollection =
        registry.listEnrichmentStrategies();
    enrichmentStrategyCollection.iterable().forEach(System.out::println);
    System.out.println("\n");

    System.out.println("Enrichments Broken: Result");
    System.out.println("-----------------------------------------");
    // unfortunately, nakadi has a bug here.
    ValidationStrategyCollection validationStrategyCollection =
        registry.listValidationStrategies();
    validationStrategyCollection.iterable().forEach(System.out::println);

  }

}

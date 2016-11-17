package nakadi.examples.metrics;

import java.util.Map;
import nakadi.Metrics;
import nakadi.MetricsResource;
import nakadi.NakadiClient;

public class MetricMain {

  public static void main(String[] args) {
    String baseURI = "http://localhost:" + 9080;

    NakadiClient client = NakadiClient.newBuilder().baseURI(baseURI).build();

    listMetrics(client);

    // supply a metric collector to the builder that just prints when called
    NakadiClient metricClient = NakadiClient.newBuilder()
        .metricCollector(new PrintingMetricCollector())
        .baseURI(baseURI)
        .build();

  }

  private static void listMetrics(NakadiClient client) {
    MetricsResource metricsResource = client.resources().metrics();
    Metrics metrics = metricsResource.get();

    System.out.println("Metric data: Result");
    System.out.println("-----------------------------------------");
    Map<String, Object> items = metrics.items();
    System.out.println(items);
  }
}

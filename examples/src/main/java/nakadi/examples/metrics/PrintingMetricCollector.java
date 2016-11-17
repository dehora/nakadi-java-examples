package nakadi.examples.metrics;

import java.util.concurrent.TimeUnit;
import nakadi.MetricCollector;

class PrintingMetricCollector implements MetricCollector {

  @Override public void mark(Meter meter) {
    System.out.println("mark: " + meter.toString());
  }

  @Override public void mark(Meter meter, long count) {
    System.out.println("mark: " + meter.toString() + " count:"+count);
  }

  @Override public void duration(Timer metric, long duration, TimeUnit unit) {
    System.out.println("duration: " + metric.toString()
        + " duration:"+duration+" unit:"+unit.name().toLowerCase());
  }
}

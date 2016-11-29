package nakadi.examples.streams;

import java.util.List;
import java.util.Map;
import nakadi.BusinessEventMapped;
import nakadi.DataChangeEvent;
import nakadi.StreamBatch;
import nakadi.StreamBatchRecord;
import nakadi.StreamCursorContext;
import nakadi.StreamObserverBackPressure;
import nakadi.StreamOffsetObserver;
import nakadi.examples.events.PriorityRequisition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BusinessEventStreamObserver
    extends StreamObserverBackPressure<BusinessEventMapped<PriorityRequisition>> {

  private static final Logger logger = LoggerFactory.getLogger(BusinessEventStreamObserver.class);

  @Override public void onStart() {
    logger.info("onStart");
  }

  @Override public void onStop() {
    logger.info("onStop");
  }

  @Override public void onCompleted() {
    logger.info("onCompleted");
  }

  @Override public void onError(Throwable t) {
    logger.info("onError {} {}", t.getMessage(),
        Thread.currentThread().getName());
    if (t instanceof InterruptedException) {
      Thread.currentThread().interrupt();
    }
  }

  @Override public void onNext(StreamBatchRecord<BusinessEventMapped<PriorityRequisition>> record) {
    final StreamOffsetObserver offsetObserver = record.streamOffsetObserver();
    final StreamBatch<BusinessEventMapped<PriorityRequisition>> batch = record.streamBatch();
    final StreamCursorContext cursor = record.streamCursorContext();

    logger.info("partition: {} ------------- ", cursor.cursor().partition());

    if (batch.isEmpty()) {
      logger.info("partition: {} keepalive", cursor.cursor().partition());
    } else {
      final List<BusinessEventMapped<PriorityRequisition>> events = batch.events();
      for (BusinessEventMapped<PriorityRequisition> event : events) {
        int hashCode = event.hashCode();
        logger.info("{} event ------------- ", hashCode);
        logger.info("{} metadata: {} ", hashCode, event.metadata());
        logger.info("{} data: {} ", hashCode, event.data());
      }
    }

    offsetObserver.onNext(record.streamCursorContext());
  }
}


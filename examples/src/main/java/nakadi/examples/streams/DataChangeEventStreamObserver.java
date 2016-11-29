package nakadi.examples.streams;

import java.util.List;
import java.util.Map;
import nakadi.DataChangeEvent;
import nakadi.StreamBatch;
import nakadi.StreamBatchRecord;
import nakadi.StreamCursorContext;
import nakadi.StreamObserverBackPressure;
import nakadi.StreamOffsetObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataChangeEventStreamObserver
    extends StreamObserverBackPressure<DataChangeEvent<Map<String, Object>>> {

  private static final Logger logger = LoggerFactory.getLogger(DataChangeEventStreamObserver.class);

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
    logger.info("DataChangeEventStreamObserver.onError {} {}", t.getMessage(),
        Thread.currentThread().getName());
    if (t instanceof InterruptedException) {
      Thread.currentThread().interrupt();
    }
  }

  @Override public void onNext(StreamBatchRecord<DataChangeEvent<Map<String, Object>>> record) {
    final StreamOffsetObserver offsetObserver = record.streamOffsetObserver();
    final StreamBatch<DataChangeEvent<Map<String, Object>>> batch = record.streamBatch();
    final StreamCursorContext cursor = record.streamCursorContext();

    logger.info("partition: {} ------------- ", cursor.cursor().partition());

    if (batch.isEmpty()) {
      logger.info("partition: {} keepalive", cursor.cursor().partition());
    } else {
      final List<DataChangeEvent<Map<String, Object>>> events = batch.events();
      for (DataChangeEvent<Map<String, Object>> event : events) {
        int hashCode = event.hashCode();
        logger.info("{} event ------------- ", hashCode);
        logger.info("{} metadata: {} ", hashCode, event.metadata());
        logger.info("{} op: {} ", hashCode, event.op());
        logger.info("{} dataType: {} ", hashCode, event.dataType());
        logger.info("{} data: {} ", hashCode, event.data());
      }
    }

    offsetObserver.onNext(record.streamCursorContext());
  }
}


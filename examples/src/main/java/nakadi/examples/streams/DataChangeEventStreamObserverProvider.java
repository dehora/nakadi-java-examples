package nakadi.examples.streams;

import java.util.Map;
import nakadi.DataChangeEvent;
import nakadi.StreamObserver;
import nakadi.StreamObserverProvider;
import nakadi.TypeLiteral;

public class DataChangeEventStreamObserverProvider
    implements StreamObserverProvider<DataChangeEvent<Map<String, Object>>> {

  @Override public StreamObserver<DataChangeEvent<Map<String, Object>>> createStreamObserver() {
    return new DataChangeEventStreamObserver();
  }

  @Override public TypeLiteral<DataChangeEvent<Map<String, Object>>> typeLiteral() {
    return new TypeLiteral<DataChangeEvent<Map<String, Object>>>() {
    };
  }
}

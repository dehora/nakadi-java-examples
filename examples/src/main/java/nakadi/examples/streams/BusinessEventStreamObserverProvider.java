package nakadi.examples.streams;

import nakadi.BusinessEventMapped;
import nakadi.StreamObserver;
import nakadi.StreamObserverProvider;
import nakadi.TypeLiteral;
import nakadi.examples.events.PriorityRequisition;

public class BusinessEventStreamObserverProvider
    implements StreamObserverProvider<BusinessEventMapped<PriorityRequisition>> {

  @Override public StreamObserver<BusinessEventMapped<PriorityRequisition>> createStreamObserver() {
    return new BusinessEventStreamObserver();
  }

  @Override public TypeLiteral<BusinessEventMapped<PriorityRequisition>> typeLiteral() {
    return new TypeLiteral<BusinessEventMapped<PriorityRequisition>>() {
    };
  }
}

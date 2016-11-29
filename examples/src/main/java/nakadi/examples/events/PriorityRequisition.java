package nakadi.examples.events;

import java.util.Objects;

public class PriorityRequisition {
  String id;

  public PriorityRequisition(String id) {
    this.id = id;
  }

  @Override public String toString() {
    return "PriorityRequisition{" + "id='" + id + '\'' +
        '}';
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PriorityRequisition that = (PriorityRequisition) o;
    return Objects.equals(id, that.id);
  }

  @Override public int hashCode() {
    return Objects.hash(id);
  }
}

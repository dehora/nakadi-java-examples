package nakadi.examples.oauth;

import java.util.Optional;
import nakadi.TokenProvider;

public class MyTokenProvider implements TokenProvider {

  /**
   * @return a value suitable for use in an Authorization header, or null to suppress the
   * Authorization header being set
   */
  @Override public Optional<String> authHeaderValue(String scope) {

    if("gordian-blade-scope".equals(scope)) {
      return Optional.of("code-gate-token");
    }

    return Optional.of("icebreaker-uid-token");
  }
}

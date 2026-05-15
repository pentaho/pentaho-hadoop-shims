package org.pentaho.hadoop.shim.common.format;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SensitiveLoggingUtilsTest {

  @Test
  public void sanitizeForLog_uriCredentials() {
    String value = "s3n://user:secret@host/path";

    assertEquals( "s3n://user:***@host/path", SensitiveLoggingUtils.sanitizeForLog( value ) );
  }

  @Test
  public void sanitizeForLog_authorityStyleCredentials() {
    String value = "user:secret@host/path";

    assertEquals( "user:***@host/path", SensitiveLoggingUtils.sanitizeForLog( value ) );
  }

  @Test
  public void sanitizeForLog_multipleCredentialOccurrences() {
    String value = "Primary=s3n://user1:secret1@host1/path Secondary=s3n://user2:secret2@host2/path";

    assertEquals(
      "Primary=s3n://user1:***@host1/path Secondary=s3n://user2:***@host2/path",
      SensitiveLoggingUtils.sanitizeForLog( value ) );
  }

  @Test
  public void sanitizeForLog_usernameContainingAtInAuthority() {
    String value = "user.name@example.com:fakePassword123@cluster-host";

    assertEquals(
      "user.name@example.com:***@cluster-host",
      SensitiveLoggingUtils.sanitizeForLog( value ) );
  }

  @Test
  public void sanitizeForLog_usernameContainingAtInUri() {
    String value = "gs://user.name@example.com:fakePassword123@cluster-host/path";

    assertEquals(
      "gs://user.name@example.com:***@cluster-host/path",
      SensitiveLoggingUtils.sanitizeForLog( value ) );
  }

  @Test
  public void sanitizeForLog_exceptionMessageShape() {
    String value =
      "java.lang.IllegalArgumentException: Does not contain a valid host:port authority: "
        + "user.name@example.com:fakePassword123@cluster-host";

    assertEquals(
      "java.lang.IllegalArgumentException: Does not contain a valid host:port authority: "
        + "user.name@example.com:***@cluster-host",
      SensitiveLoggingUtils.sanitizeForLog( value ) );
  }

  @Test
  public void sanitizeForLog_withoutCredentials() {
    String value = "s3n://host/path";

    assertEquals( value, SensitiveLoggingUtils.sanitizeForLog( value ) );
  }

  @Test
  public void sanitizeForLog_nullInput() {
    assertNull( SensitiveLoggingUtils.sanitizeForLog( null ) );
  }
}

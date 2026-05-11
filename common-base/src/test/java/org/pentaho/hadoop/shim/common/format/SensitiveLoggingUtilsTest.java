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
  public void sanitizeForLog_withoutCredentials() {
    String value = "s3n://host/path";

    assertEquals( value, SensitiveLoggingUtils.sanitizeForLog( value ) );
  }

  @Test
  public void sanitizeForLog_nullInput() {
    assertNull( SensitiveLoggingUtils.sanitizeForLog( null ) );
  }
}

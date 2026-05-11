package org.pentaho.hadoop.shim.common.format;

import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;

import java.util.regex.Pattern;

public final class SensitiveLoggingUtils {

  private static final LogChannelInterface logger = LogChannel.GENERAL;
  private static final Pattern URI_CREDENTIALS_PATTERN =
    Pattern.compile( "(://[^\\s:/?#]+:)([^@\\s/?#]+)(@)" );
  private static final Pattern AUTHORITY_CREDENTIALS_PATTERN =
    Pattern.compile( "(^[^\\s:@]+:)([^@\\s/?#]+)(@)" );

  private SensitiveLoggingUtils() {
  }

  public static String sanitizeForLog( String value ) {
    if ( value == null ) {
      return null;
    }

    String sanitized = URI_CREDENTIALS_PATTERN.matcher( value ).replaceAll( "$1***$3" );
    sanitized = AUTHORITY_CREDENTIALS_PATTERN.matcher( sanitized ).replaceAll( "$1***$3" );
    return sanitized;
  }

  public static void logSanitizedInitializationError( String message, String path, Exception e ) {
    logger.logError( message + ": " + sanitizeForLog( path ) );
    logger.logError( e.toString() );
  }
}


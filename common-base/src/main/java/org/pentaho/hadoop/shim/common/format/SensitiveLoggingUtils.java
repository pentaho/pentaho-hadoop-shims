/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2026 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

package org.pentaho.hadoop.shim.common.format;

import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class SensitiveLoggingUtils {

  private static final LogChannelInterface logger = LogChannel.GENERAL;
  private static final String REDACTED = "***";

  /**
   * Matches either:
   * - scheme://authority
   * - authority
   *
   * up to a delimiter like slash, whitespace, ?, or #.
   */
  private static final Pattern AUTHORITY_CANDIDATE_PATTERN =
    Pattern.compile( "(?:[a-zA-Z][a-zA-Z0-9+.-]*://)?[^\\s/?#]+" );

  private SensitiveLoggingUtils() {
  }

  public static String sanitizeForLog( String value ) {
    if ( value == null ) {
      return null;
    }

    Matcher matcher = AUTHORITY_CANDIDATE_PATTERN.matcher( value );
    StringBuffer result = new StringBuffer();

    while ( matcher.find() ) {
      String candidate = matcher.group();
      String sanitized = sanitizeCandidate( candidate );
      matcher.appendReplacement( result, Matcher.quoteReplacement( sanitized ) );
    }
    matcher.appendTail( result );

    return result.toString();
  }

  private static String sanitizeCandidate( String candidate ) {
    int schemeIdx = candidate.indexOf( "://" );
    int authorityStart = schemeIdx >= 0 ? schemeIdx + 3 : 0;

    if ( authorityStart >= candidate.length() ) {
      return candidate;
    }

    String prefix = candidate.substring( 0, authorityStart );
    String authority = candidate.substring( authorityStart );

    int atIndex = authority.lastIndexOf( '@' );
    if ( atIndex < 0 ) {
      return candidate;
    }

    int colonIndex = authority.lastIndexOf( ':', atIndex );
    if ( colonIndex < 0 || colonIndex + 1 >= atIndex ) {
      return candidate;
    }

    String sanitizedAuthority =
      authority.substring( 0, colonIndex + 1 ) + REDACTED + authority.substring( atIndex );

    return prefix + sanitizedAuthority;
  }

  public static void logSanitizedInitializationError( String message, String path, Exception e ) {
    logger.logError( message + ": " + sanitizeForLog( path ) );
    if ( e != null ) {
      String exceptionMessage = e.getMessage();
      if ( exceptionMessage == null ) {
        logger.logError( "Cause: " + e.getClass().getSimpleName() );
      } else {
        logger.logError( "Cause: " + e.getClass().getSimpleName() + ": " + sanitizeForLog( exceptionMessage ) );
      }
    }
  }

  public static IllegalStateException sanitizedIllegalStateException( String message, Exception e ) {
    if ( e == null ) {
      return new IllegalStateException( message );
    }

    String exceptionMessage = e.getMessage();
    if ( exceptionMessage == null ) {
      return new IllegalStateException( message + " Cause: " + e.getClass().getSimpleName() );
    }

    return new IllegalStateException(
      message + " Cause: " + e.getClass().getSimpleName() + ": " + sanitizeForLog( exceptionMessage ) );
  }
}

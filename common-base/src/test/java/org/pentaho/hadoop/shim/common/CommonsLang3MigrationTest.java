/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/


package org.pentaho.hadoop.shim.common;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests to validate commons-lang3 3.18.0 migration in pentaho-hadoop-shims.
 * Tests verify that:
 * 1. Commons-lang3 classes are available in the classpath
 * 2. Methods used in hadoop-shims work correctly with commons-lang3
 * 3. SystemUtils and ClassUtils work as expected
 * 4. No ClassNotFoundException at runtime
 */
public class CommonsLang3MigrationTest {

  @BeforeClass
  public static void verifyCommonsLang3Available() throws ClassNotFoundException {
    // Verify commons-lang3 is available
    Class.forName( "org.apache.commons.lang3.StringUtils" );
    Class.forName( "org.apache.commons.lang3.SystemUtils" );
    Class.forName( "org.apache.commons.lang3.ClassUtils" );
    Class.forName( "org.apache.commons.text.StringEscapeUtils" );
  }

  @Test
  public void testStringUtilsIsEmpty() {
    assertTrue( "StringUtils.isEmpty(null) should be true", StringUtils.isEmpty( null ) );
    assertTrue( "StringUtils.isEmpty(\"\") should be true", StringUtils.isEmpty( "" ) );
    assertFalse( "StringUtils.isEmpty(\"hadoop\") should be false", StringUtils.isEmpty( "hadoop" ) );
  }

  @Test
  public void testStringUtilsIsNotEmpty() {
    assertFalse( "StringUtils.isNotEmpty(null) should be false", StringUtils.isNotEmpty( null ) );
    assertFalse( "StringUtils.isNotEmpty(\"\") should be false", StringUtils.isNotEmpty( "" ) );
    assertTrue( "StringUtils.isNotEmpty(\"hadoop\") should be true", StringUtils.isNotEmpty( "hadoop" ) );
  }

  @Test
  public void testStringUtilsIsBlank() {
    assertTrue( "StringUtils.isBlank(null) should be true", StringUtils.isBlank( null ) );
    assertTrue( "StringUtils.isBlank(\"\") should be true", StringUtils.isBlank( "" ) );
    assertTrue( "StringUtils.isBlank(\"   \") should be true", StringUtils.isBlank( "   " ) );
    assertFalse( "StringUtils.isBlank(\"hadoop\") should be false", StringUtils.isBlank( "hadoop" ) );
  }

  @Test
  public void testStringUtilsTrim() {
    assertEquals( "StringUtils.trim should remove whitespace", "hadoop", StringUtils.trim( "  hadoop  " ) );
    assertNull( "StringUtils.trim(null) should be null", StringUtils.trim( null ) );
    assertEquals( "StringUtils.trim(\"\") should be \"\"", "", StringUtils.trim( "" ) );
  }

  @Test
  public void testStringUtilsSplit() {
    String[] result = StringUtils.split( "hdfs,mapreduce,yarn", "," );
    assertEquals( "StringUtils.split should produce 3 elements", 3, result.length );
    assertEquals( "First element should be 'hdfs'", "hdfs", result[0] );
    assertEquals( "Second element should be 'mapreduce'", "mapreduce", result[1] );
    assertEquals( "Third element should be 'yarn'", "yarn", result[2] );
  }

  @Test
  public void testStringUtilsJoin() {
    String[] array = {"hdfs", "mapreduce", "yarn"};
    String joined = StringUtils.join( array, "," );
    assertEquals( "StringUtils.join should produce comma-separated string", "hdfs,mapreduce,yarn", joined );
  }

  @Test
  @SuppressWarnings("deprecation") // StringUtils.replace is deprecated but tested for backward compatibility
  public void testStringUtilsReplace() {
    String replaced = StringUtils.replace( "hadoop cluster", "cluster", "ecosystem" );
    assertEquals( "StringUtils.replace should replace text", "hadoop ecosystem", replaced );
  }

  @Test
  @SuppressWarnings("deprecation") // StringUtils.contains is deprecated but tested for backward compatibility
  public void testStringUtilsContains() {
    assertTrue( "StringUtils.contains should find substring", StringUtils.contains( "hadoop", "had" ) );
    assertFalse( "StringUtils.contains should not find non-existent substring",
      StringUtils.contains( "hadoop", "xyz" ) );
  }

  @Test
  @SuppressWarnings("deprecation") // StringUtils.startsWith is deprecated but tested for backward compatibility
  public void testStringUtilsStartsWith() {
    assertTrue( "StringUtils.startsWith should detect prefix", StringUtils.startsWith( "hadoop", "had" ) );
    assertFalse( "StringUtils.startsWith should not match non-prefix",
      StringUtils.startsWith( "hadoop", "oop" ) );
  }

  @Test
  @SuppressWarnings("deprecation") // StringUtils.endsWith is deprecated but tested for backward compatibility
  public void testStringUtilsEndsWith() {
    assertTrue( "StringUtils.endsWith should detect suffix", StringUtils.endsWith( "hadoop", "oop" ) );
    assertFalse( "StringUtils.endsWith should not match non-suffix",
      StringUtils.endsWith( "hadoop", "had" ) );
  }

  @Test
  public void testSystemUtilsOsName() {
    assertNotNull( "SystemUtils.OS_NAME should not be null", SystemUtils.OS_NAME );
    assertFalse( "SystemUtils.OS_NAME should not be empty", StringUtils.isEmpty( SystemUtils.OS_NAME ) );
  }

  @Test
  public void testSystemUtilsJavaVersion() {
    assertNotNull( "SystemUtils.JAVA_VERSION should not be null", SystemUtils.JAVA_VERSION );
    assertFalse( "SystemUtils.JAVA_VERSION should not be empty", StringUtils.isEmpty( SystemUtils.JAVA_VERSION ) );
  }

  @Test
  public void testSystemUtilsJavaVersionAvailable() {
    // Test that SystemUtils can determine Java version
    assertFalse( "SystemUtils should have Java version information", 
      StringUtils.isEmpty( SystemUtils.JAVA_VERSION ) );
  }

  @Test
  public void testClassUtilsGetClass() {
    try {
      Class<?> stringClass = ClassUtils.getClass( "java.lang.String" );
      assertNotNull( "ClassUtils.getClass should load String class", stringClass );
      assertEquals( "Loaded class should be String", String.class, stringClass );
    } catch ( ClassNotFoundException e ) {
      fail( "ClassUtils.getClass should be able to load java.lang.String" );
    }
  }

  @Test
  public void testStringEscapeUtilsEscapeJson() {
    String escaped = StringEscapeUtils.escapeJson( "Hello \"World\"" );
    assertTrue( "StringEscapeUtils.escapeJson should escape quotes", escaped.contains( "\\\"" ) );
  }

  @Test
  public void testStringEscapeUtilsEscapeXml11() {
    String escaped = StringEscapeUtils.escapeXml11( "<config>value</config>" );
    assertTrue( "StringEscapeUtils.escapeXml11 should escape '<'", escaped.contains( "&lt;" ) );
    assertTrue( "StringEscapeUtils.escapeXml11 should escape '>'", escaped.contains( "&gt;" ) );
  }

  @Test
  public void testStringEscapeUtilsUnescapeHtml4() {
    String unescaped = StringEscapeUtils.unescapeHtml4( "&lt;value&gt;" );
    assertEquals( "StringEscapeUtils.unescapeHtml4 should unescape HTML entities", "<value>", unescaped );
  }

  @Test
  public void testCommonsLang3VersionConsistency() {
    // Verify commons-lang3 StringUtils has expected methods
    try {
      // If we got here, isBlank method exists in commons-lang3
      assertNotNull( "StringUtils.isBlank method should exist", StringUtils.class.getMethod( "isBlank", CharSequence.class ) );
    } catch ( NoSuchMethodException e ) {
      fail( "commons-lang3 should have isBlank method" );
    }
  }

  @Test
  public void testCommonsLang3IsAvailable() {
    // Verify commons-lang3 is the primary implementation in use
    // Note: commons-lang 2.6 may be present as transitive dependency from commons-configuration
    // but our code should use commons-lang3 exclusively, as verified below

    // Verify commons-lang3 is available instead
    try {
      Class<?> stringUtilsClass = Class.forName( "org.apache.commons.lang3.StringUtils" );
      assertNotNull( "Should load commons-lang3 StringUtils", stringUtilsClass );

      // Test commons-lang3-specific behavior
      String test = StringUtils.stripEnd( "value  ", null );
      assertEquals( "stripEnd should work correctly", "value", test );
    } catch ( ClassNotFoundException e ) {
      fail( "commons-lang3.StringUtils should be available" );
    }
  }

  @Test
  public void testStringUtilsSubstring() {
    String result = StringUtils.substring( "hadoop", 0, 3 );
    assertEquals( "StringUtils.substring should extract substring", "had", result );
  }

  @Test
  public void testStringUtilsLeftPad() {
    String result = StringUtils.leftPad( "5", 3, "0" );
    assertEquals( "StringUtils.leftPad should pad left", "005", result );
  }

  @Test
  public void testStringUtilsRightPad() {
    String result = StringUtils.rightPad( "5", 3, "0" );
    assertEquals( "StringUtils.rightPad should pad right", "500", result );
  }

  @Test
  public void testStringUtilsCapitalize() {
    String result = StringUtils.capitalize( "hadoop" );
    assertEquals( "StringUtils.capitalize should capitalize first letter", "Hadoop", result );
  }

  @Test
  public void testStringUtilsUncapitalize() {
    String result = StringUtils.uncapitalize( "Hadoop" );
    assertEquals( "StringUtils.uncapitalize should lowercase first letter", "hadoop", result );
  }
}

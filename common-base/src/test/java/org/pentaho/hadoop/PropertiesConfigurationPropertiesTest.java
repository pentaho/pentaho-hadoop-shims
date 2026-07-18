/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.hadoop;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.VFS;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class PropertiesConfigurationPropertiesTest {
  private PropertiesConfiguration propertiesConfiguration;
  private PropertiesConfigurationProperties propertiesConfigurationProperties;

  @Before
  public void setup() {
    propertiesConfiguration = mock( PropertiesConfiguration.class );
    propertiesConfigurationProperties = new PropertiesConfigurationProperties( propertiesConfiguration );
  }

  @Test
  public void testGetProperty() {
    String key = "key";
    String value = "value";
    when( propertiesConfiguration.getString( key, null ) ).thenReturn( value );
    assertEquals( value, propertiesConfigurationProperties.getProperty( key ) );
  }

  @Test
  public void testGetPropertyDefault() {
    String key = "key";
    String defaultValue = "default";
    String value = "value";
    when( propertiesConfiguration.getString( key, defaultValue ) ).thenReturn( value );
    assertEquals( value, propertiesConfigurationProperties.getProperty( key, defaultValue ) );
  }

  @Test
  public void testGetString() {
    String key = "key";
    String value = "value";
    when( propertiesConfiguration.getProperty( key ) ).thenReturn( value );
    assertEquals( value, propertiesConfigurationProperties.get( key ) );
  }

  @Test
  public void testGetObject() {
    assertNull( propertiesConfigurationProperties.get( new Object() ) );
    verifyNoMoreInteractions( propertiesConfiguration );
  }

  @Test
  public void testGetNull() {
    String value = "value";
    when( propertiesConfiguration.getProperty( null ) ).thenReturn( value );
    assertEquals( value, propertiesConfigurationProperties.get( null ) );
  }

  @Test
  public void testSetProperty() {
    String key = "key";
    String value = "value";
    String previous = "prev";
    when( propertiesConfiguration.getProperty( key ) ).thenReturn( previous );
    assertEquals( previous, propertiesConfigurationProperties.put( key, value ) );
    verify( propertiesConfiguration ).setProperty( key, value );
  }

  @Test( expected = IllegalArgumentException.class )
  public void testSetPropertyNullKey() {
    propertiesConfigurationProperties.setProperty( null, "value" );
  }

  @Test
  public void stringPropertyNames() {
    final Set<String> names = new HashSet<>( Arrays.asList( "a", "b", "c" ) );
    mockKeys( names );
    assertEquals( names, new HashSet<String>( propertiesConfigurationProperties.stringPropertyNames() ) );
  }

  @Test
  public void testKeySet() {
    final Set<String> names = new HashSet<>( Arrays.asList( "a", "b", "c" ) );
    mockKeys( names );
    assertEquals( new HashSet<Object>( names ), new HashSet<>( propertiesConfigurationProperties.keySet() ) );
  }

  @Test
  public void testEntrySet() {
    Map<String, Object> sourceMap = new HashMap<>();
    sourceMap.put( "a", "b" );
    sourceMap.put( "c", "d" );
    mockToMap( sourceMap );
    assertEquals( sourceMap.entrySet(), propertiesConfigurationProperties.entrySet() );
  }

  @Test
  public void testSize() {
    final Set<String> names = new HashSet<>( Arrays.asList( "a", "b", "c" ) );
    mockKeys( names );
    assertEquals( names.size(), propertiesConfigurationProperties.size() );
  }

  @Test
  public void testIsEmptyTrue() {
    final Set<String> names = new HashSet<>();
    mockKeys( names );
    assertTrue( propertiesConfigurationProperties.isEmpty() );
  }

  @Test
  public void testIsEmptyFalse() {
    final Set<String> names = new HashSet<>( Arrays.asList( "a", "b", "c" ) );
    mockKeys( names );
    assertFalse( propertiesConfigurationProperties.isEmpty() );
  }

  @Test
  public void testKeys() {
    HashSet<String> names = new HashSet<>( Arrays.asList( "a", "b", "c" ) );
    mockKeys( names );
    assertEquals( names, new HashSet<>( Collections.list( propertiesConfigurationProperties.keys() ) ) );
  }

  @Test
  public void testElements() {
    Map<String, Object> sourceMap = new HashMap<>();
    sourceMap.put( "a", "b" );
    sourceMap.put( "c", "d" );
    mockToMap( sourceMap );
    assertEquals( new HashSet<>( sourceMap.values() ),
      new HashSet<>( Collections.list( propertiesConfigurationProperties.elements() ) ) );
  }

  @Test
  public void testContainsTrue() {
    Map<String, Object> sourceMap = new HashMap<>();
    sourceMap.put( "a", "b" );
    String d = "d";
    sourceMap.put( "c", d );
    mockToMap( sourceMap );
    assertTrue( propertiesConfigurationProperties.contains( d ) );
    assertTrue( propertiesConfigurationProperties.containsValue( d ) );
  }

  @Test
  public void testContainsFalse() {
    Map<String, Object> sourceMap = new HashMap<>();
    sourceMap.put( "a", "b" );
    sourceMap.put( "c", "d" );
    mockToMap( sourceMap );
    assertFalse( propertiesConfigurationProperties.containsValue( "E" ) );
  }

  @Test
  public void testPropertyNames() {
    HashSet<String> names = new HashSet<>( Arrays.asList( "a", "b", "c" ) );
    mockKeys( names );

    HashSet<String> propertyNames = Collections.list( propertiesConfigurationProperties.propertyNames() ).stream()
      .map( Object::toString )
      .collect( Collectors.toCollection( HashSet::new ) );
    assertEquals( names, propertyNames );
  }

  @Test
  public void testValues() {
    Map<String, Object> sourceMap = new HashMap<>();
    sourceMap.put( "a", "b" );
    sourceMap.put( "c", "d" );
    mockToMap( sourceMap );
    assertEquals( new HashSet<>( sourceMap.values() ),
      new HashSet<Object>( propertiesConfigurationProperties.values() ) );
  }

  @Test
  public void testContainsKey() {
    String a = "a";
    HashSet<String> names = new HashSet<>( Arrays.asList( a, "b", "c" ) );
    mockKeys( names );
    assertTrue( propertiesConfigurationProperties.containsKey( a ) );
    assertFalse( propertiesConfigurationProperties.containsKey( "d" ) );
    assertFalse( propertiesConfigurationProperties.containsKey( new Object() ) );
    assertFalse( propertiesConfigurationProperties.containsKey( null ) );
  }

  @Test
  public void testRemove() {
    String key = "key";
    String prev = "prev";
    when( propertiesConfiguration.getProperty( key ) ).thenReturn( prev );
    assertEquals( prev, propertiesConfigurationProperties.remove( key ) );
    verify( propertiesConfiguration ).clearProperty( key );
  }

  @Test
  public void testRemoveObject() {
    assertNull( propertiesConfigurationProperties.remove( new Object() ) );
    verifyNoMoreInteractions( propertiesConfiguration );
  }

  @Test
  public void testPutAll() {
    Map<String, Object> sourceMap = new HashMap<>();
    sourceMap.put( "a", "b" );
    sourceMap.put( "c", "d" );
    propertiesConfigurationProperties.putAll( sourceMap );
    for ( Map.Entry<String, Object> stringObjectEntry : sourceMap.entrySet() ) {
      verify( propertiesConfiguration ).setProperty( stringObjectEntry.getKey(), stringObjectEntry.getValue() );
    }
  }

  @Test
  public void testClear() {
    propertiesConfigurationProperties.clear();
    verify( propertiesConfiguration ).clear();
  }

  @Test( expected = UnsupportedOperationException.class )
  public void testLoadReader() throws IOException {
    propertiesConfigurationProperties.load( mock( Reader.class ) );
  }

  @Test( expected = UnsupportedOperationException.class )
  public void testLoadInputStream() throws IOException {
    propertiesConfigurationProperties.load( mock( InputStream.class ) );
  }

  @Test( expected = UnsupportedOperationException.class )
  public void testSaveOutputStream() {
    propertiesConfigurationProperties.save( mock( OutputStream.class ), "" );
  }

  @Test( expected = UnsupportedOperationException.class )
  public void testStoreWriter() throws IOException {
    propertiesConfigurationProperties.store( mock( Writer.class ), "" );
  }

  @Test( expected = UnsupportedOperationException.class )
  public void testStoreOutputStream() throws IOException {
    propertiesConfigurationProperties.store( mock( OutputStream.class ), "" );
  }

  @Test( expected = UnsupportedOperationException.class )
  public void testStoreToXmlNoEncoding() throws IOException {
    propertiesConfigurationProperties.storeToXML( mock( OutputStream.class ), "" );
  }

  @Test( expected = UnsupportedOperationException.class )
  public void testStoreToXmlEncoding() throws IOException {
    propertiesConfigurationProperties.storeToXML( mock( OutputStream.class ), "", "UTF-8" );
  }

  @Test( expected = UnsupportedOperationException.class )
  public void testListPrintStream() {
    propertiesConfigurationProperties.list( mock( PrintStream.class ) );
  }

  @Test( expected = UnsupportedOperationException.class )
  public void testListPrintWriter() {
    propertiesConfigurationProperties.list( mock( PrintWriter.class ) );
  }

  @Test( expected = UnsupportedOperationException.class )
  public void testHash() {
    propertiesConfigurationProperties.rehash();
  }

  @Test( expected = UnsupportedOperationException.class )
  public void testClone() {
    propertiesConfigurationProperties.clone();
  }

  // --- commons-configuration2 file-backed (builder) path coverage ---

  @Test
  public void fileBackedConstructorReadsValues() throws Exception {
    File file = File.createTempFile( "config", ".properties" );
    Files.write( file.toPath(), "name=Test Cluster\nfoo=bar\n".getBytes( StandardCharsets.UTF_8 ) );
    try {
      PropertiesConfigurationProperties props = new PropertiesConfigurationProperties( resolve( file ) );
      assertEquals( "bar", props.getProperty( "foo" ) );
      assertEquals( "Test Cluster", props.getProperty( "name" ) );
      assertTrue( props.containsKey( "foo" ) );
      assertFalse( props.isEmpty() );
    } finally {
      Files.deleteIfExists( file.toPath() );
    }
  }

  @Test
  public void fileBackedConstructorDefaultsWhenKeyMissing() throws Exception {
    File file = File.createTempFile( "config", ".properties" );
    Files.write( file.toPath(), "foo=bar\n".getBytes( StandardCharsets.UTF_8 ) );
    try {
      PropertiesConfigurationProperties props = new PropertiesConfigurationProperties( resolve( file ) );
      assertEquals( "fallback", props.getProperty( "missing", "fallback" ) );
    } finally {
      Files.deleteIfExists( file.toPath() );
    }
  }

  @Test
  public void fileBackedConstructorAllowsMissingFile() throws Exception {
    // allowFailOnInit=true must degrade gracefully (matching legacy cfg1 lenient behavior) rather than throw.
    File file = new File( System.getProperty( "java.io.tmpdir" ),
      "does-not-exist-" + System.nanoTime() + ".properties" );
    PropertiesConfigurationProperties props = new PropertiesConfigurationProperties( resolve( file ) );
    assertTrue( props.isEmpty() );
    assertNull( props.getProperty( "anything" ) );
  }

  @Test
  public void fileBackedPutPersistsAndReReads() throws Exception {
    File file = File.createTempFile( "config", ".properties" );
    Files.write( file.toPath(), "foo=bar\n".getBytes( StandardCharsets.UTF_8 ) );
    try {
      PropertiesConfigurationProperties props = new PropertiesConfigurationProperties( resolve( file ) );
      props.put( "new.key", "new.value" );
      assertEquals( "new.value", props.getProperty( "new.key" ) );
    } finally {
      Files.deleteIfExists( file.toPath() );
    }
  }

  private static FileObject resolve( File file ) throws Exception {
    return VFS.getManager().resolveFile( file.toURI().toString() );
  }

  private void mockKeys( final Set<String> keys ) {
    when( propertiesConfiguration.getKeys() ).thenAnswer( new Answer<Iterator<String>>() {
      @Override public Iterator<String> answer( InvocationOnMock invocation ) throws Throwable {
        return keys.iterator();
      }
    } );
    when( propertiesConfiguration.containsKey( anyString() ) ).thenAnswer( new Answer<Boolean>() {
      @Override public Boolean answer( InvocationOnMock invocation ) throws Throwable {
        return keys.contains( invocation.getArguments()[ 0 ] );
      }
    } );
  }

  private void mockToMap( final Map<String, Object> map ) {
    mockKeys( map.keySet() );
    for ( Map.Entry<String, Object> stringObjectEntry : map.entrySet() ) {
      when( propertiesConfiguration.getProperty( stringObjectEntry.getKey() ) )
        .thenReturn( stringObjectEntry.getValue() );
    }
  }
}

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


package org.pentaho.hadoop.shim.common.fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;
import org.pentaho.hadoop.shim.api.internal.fs.Path;

public class FileSystemProxyTest {

  @Test( expected = NullPointerException.class )
  public void instantiation_null_delegate() throws IOException {
    FileSystemProxy proxy = new FileSystemProxy( null );
    if ( proxy != null ) {
      proxy.close();
    }
  }

  private Configuration getLocalFileSystemConfiguration() {
    Configuration c = new Configuration();
    c.set( "fs.default.name", "file:///" );
    return c;
  }

  @Test
  public void getDelegate() throws IOException {
    FileSystem delegate = FileSystem.get( new Configuration() );
    FileSystemProxy proxy = new FileSystemProxy( delegate );
    assertEquals( delegate, proxy.getDelegate() );
    if ( proxy != null ) {
      proxy.close();
    }
  }

  @Test
  public void asPath_String() throws IOException, URISyntaxException {
    Configuration c = getLocalFileSystemConfiguration();
    FileSystem delegate = FileSystem.get( c );
    FileSystemProxy proxy = new FileSystemProxy( delegate );
    Path p = proxy.asPath( "/" );
    assertNotNull( p );
    assertEquals( new URI( "/" ), p.toUri() );
    if ( proxy != null ) {
      proxy.close();
    }
  }

  @Test
  public void asPath_Path_String() throws IOException, URISyntaxException {
    Configuration c = getLocalFileSystemConfiguration();
    FileSystem delegate = FileSystem.get( c );
    FileSystemProxy proxy = new FileSystemProxy( delegate );
    Path p = proxy.asPath( "/" );
    assertNotNull( p );
    assertEquals( new URI( "/" ), p.toUri() );

    Path test = proxy.asPath( p, "test" );
    assertNotNull( test );
    assertEquals( new URI( "/test" ), test.toUri() );
    if ( proxy != null ) {
      proxy.close();
    }
  }

  @Test
  public void asPath_String_String() throws IOException, URISyntaxException {
    Configuration c = getLocalFileSystemConfiguration();
    FileSystem delegate = FileSystem.get( c );
    FileSystemProxy proxy = new FileSystemProxy( delegate );
    Path p = proxy.asPath( "/", "test" );
    assertNotNull( p );
    assertEquals( new URI( "/test" ), p.toUri() );
    if ( proxy != null ) {
      proxy.close();
    }
  }

  @Test
  public void asPath_exists() throws IOException, URISyntaxException {
    Configuration c = getLocalFileSystemConfiguration();
    FileSystem delegate = FileSystem.get( c );
    FileSystemProxy proxy = new FileSystemProxy( delegate );
    assertTrue( proxy.exists( proxy.asPath( "/" ) ) );
    if ( proxy != null ) {
      proxy.close();
    }
  }

  @Test
  public void asPath_delete() throws IOException, URISyntaxException {
    Configuration c = getLocalFileSystemConfiguration();

    File tmp = File.createTempFile( FileSystemProxyTest.class.getSimpleName(), null );

    FileSystem delegate = FileSystem.get( c );
    FileSystemProxy proxy = new FileSystemProxy( delegate );
    Path p = proxy.asPath( tmp.getAbsolutePath() );
    assertTrue( proxy.exists( p ) );
    proxy.delete( p, true );
    assertFalse( proxy.exists( p ) );
    assertFalse( tmp.exists() );
    if ( proxy != null ) {
      proxy.close();
    }
  }
}

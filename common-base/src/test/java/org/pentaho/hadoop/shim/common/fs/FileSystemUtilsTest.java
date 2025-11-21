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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.*;

/**
 * Unit tests for FileSystemUtils.
 */
public class FileSystemUtilsTest {

  @Before
  public void setUp() {
    // Clear registry before each test
    FileSystemRegistry.clearAll();
  }

  @After
  public void tearDown() {
    // Restore defaults after each test
    FileSystemRegistry.registerDefaults();
  }

  @Test
  public void testGetFileSystem_String() throws IOException {
    // Register a test filesystem (using local FS as it's always available)
    FileSystemRegistry.registerFileSystem( "file", "org.apache.hadoop.fs.LocalFileSystem" );

    FileSystem fs = FileSystemUtils.getFileSystem( "file:///tmp/test.txt" );

    assertNotNull( "FileSystem should not be null", fs );
    assertEquals( "Should be local filesystem", "file", fs.getUri().getScheme() );

    fs.close();
  }

  @Test
  public void testGetFileSystem_URI() throws IOException {
    FileSystemRegistry.registerFileSystem( "file", "org.apache.hadoop.fs.LocalFileSystem" );

    URI uri = URI.create( "file:///tmp/test.txt" );
    FileSystem fs = FileSystemUtils.getFileSystem( uri );

    assertNotNull( "FileSystem should not be null", fs );
    assertEquals( "Should be local filesystem", "file", fs.getUri().getScheme() );

    fs.close();
  }

  @Test
  public void testGetFileSystem_StringWithConfiguration() throws IOException {
    Configuration conf = new Configuration();
    conf.set( "custom.property", "custom.value" );

    FileSystemRegistry.registerFileSystem( "file", "org.apache.hadoop.fs.LocalFileSystem" );

    FileSystem fs = FileSystemUtils.getFileSystem( "file:///tmp/test.txt", conf );

    assertNotNull( "FileSystem should not be null", fs );
    assertEquals( "Should be local filesystem", "file", fs.getUri().getScheme() );

    // Verify that the configuration has both custom property and filesystem registration
    assertEquals( "custom.value", conf.get( "custom.property" ) );
    assertEquals( "org.apache.hadoop.fs.LocalFileSystem", conf.get( "fs.file.impl" ) );

    fs.close();
  }

  @Test
  public void testGetFileSystem_URIWithConfiguration() throws IOException {
    Configuration conf = new Configuration();
    conf.set( "custom.property", "custom.value" );

    FileSystemRegistry.registerFileSystem( "file", "org.apache.hadoop.fs.LocalFileSystem" );

    URI uri = URI.create( "file:///tmp/test.txt" );
    FileSystem fs = FileSystemUtils.getFileSystem( uri, conf );

    assertNotNull( "FileSystem should not be null", fs );
    assertEquals( "Should be local filesystem", "file", fs.getUri().getScheme() );

    // Verify that the configuration has both custom property and filesystem registration
    assertEquals( "custom.value", conf.get( "custom.property" ) );
    assertEquals( "org.apache.hadoop.fs.LocalFileSystem", conf.get( "fs.file.impl" ) );

    fs.close();
  }

  @Test
  public void testCreateConfiguration() {
    FileSystemRegistry.registerFileSystem( "test", "com.example.TestFileSystem" );

    Configuration conf = FileSystemUtils.createConfiguration();

    assertNotNull( "Configuration should not be null", conf );
    assertEquals( "com.example.TestFileSystem", conf.get( "fs.test.impl" ) );
  }

  @Test
  public void testCreateConfiguration_WithBase() {
    Configuration base = new Configuration();
    base.set( "base.property", "base.value" );

    FileSystemRegistry.registerFileSystem( "test", "com.example.TestFileSystem" );

    Configuration conf = FileSystemUtils.createConfiguration( base );

    assertNotNull( "Configuration should not be null", conf );
    assertEquals( "Should inherit base property", "base.value", conf.get( "base.property" ) );
    assertEquals( "Should have registered filesystem", "com.example.TestFileSystem", conf.get( "fs.test.impl" ) );
  }

  @Test
  public void testGetDefaultFileSystem() throws IOException {
    FileSystemRegistry.registerDefaults();

    FileSystem fs = FileSystemUtils.getDefaultFileSystem();

    assertNotNull( "Default FileSystem should not be null", fs );

    fs.close();
  }

  @Test
  public void testGetLocalFileSystem() throws IOException {
    FileSystemRegistry.registerDefaults();

    FileSystem fs = FileSystemUtils.getLocalFileSystem();

    assertNotNull( "Local FileSystem should not be null", fs );
    assertTrue( "Should be local file system",
      fs.getUri().getScheme() == null || "file".equals( fs.getUri().getScheme() ) );

    fs.close();
  }

  @Test
  public void testFileSystemUtilsAutoRegistersDefaults() throws IOException {
    // After clearing in @Before, manually register defaults to test
    FileSystemRegistry.registerDefaults();

    // Verify defaults are now registered
    assertTrue( "PVFS should be registered after calling registerDefaults",
      FileSystemRegistry.isRegistered( "pvfs" ) );
    assertTrue( "ADL should be registered after calling registerDefaults",
      FileSystemRegistry.isRegistered( "adl" ) );
  }

  @Test
  public void testMultipleFileSystemCreation() throws IOException {
    FileSystemRegistry.registerFileSystem( "file", "org.apache.hadoop.fs.LocalFileSystem" );

    // Create multiple filesystems to ensure they're independent
    FileSystem fs1 = FileSystemUtils.getFileSystem( "file:///tmp/test1.txt" );
    FileSystem fs2 = FileSystemUtils.getFileSystem( "file:///tmp/test2.txt" );

    assertNotNull( "First FileSystem should not be null", fs1 );
    assertNotNull( "Second FileSystem should not be null", fs2 );

    // They should be the same instance due to FileSystem caching
    // but still verify both work
    assertEquals( "file", fs1.getUri().getScheme() );
    assertEquals( "file", fs2.getUri().getScheme() );

    fs1.close();
    fs2.close();
  }

  @Test
  public void testConfigurationModification() throws IOException {
    Configuration conf = new Configuration();
    conf.set( "test.before", "value.before" );

    FileSystemRegistry.registerFileSystem( "test", "com.example.TestFileSystem" );

    // Get filesystem with configuration
    FileSystemUtils.getFileSystem( "file:///tmp", conf );

    // Verify configuration was modified to include filesystem registration
    assertEquals( "Original property should be preserved", "value.before", conf.get( "test.before" ) );
    assertEquals( "Filesystem should be registered in config",
      "com.example.TestFileSystem", conf.get( "fs.test.impl" ) );
  }

  @Test
  public void testURIWithoutScheme() throws IOException {
    // Test with relative path (no scheme) - should use default filesystem
    FileSystemRegistry.registerDefaults();

    FileSystem fs = FileSystemUtils.getFileSystem( "/tmp/test.txt" );

    assertNotNull( "FileSystem should not be null", fs );

    fs.close();
  }

  @Test
  public void testCreateConfigurationIsolation() {
    Configuration conf1 = FileSystemUtils.createConfiguration();
    Configuration conf2 = FileSystemUtils.createConfiguration();

    // Modify conf1
    conf1.set( "test.property", "test.value" );

    // conf2 should not have the modification
    assertNull( "Configuration instances should be isolated",
      conf2.get( "test.property" ) );
  }

  @Test
  public void testCreateConfigurationFromBaseIsolation() {
    Configuration base = new Configuration();
    base.set( "base.property", "base.value" );

    Configuration derived1 = FileSystemUtils.createConfiguration( base );
    Configuration derived2 = FileSystemUtils.createConfiguration( base );

    // Modify derived1
    derived1.set( "derived.property", "derived.value" );

    // derived2 should not have the modification
    assertNull( "Derived configuration instances should be isolated",
      derived2.get( "derived.property" ) );

    // But both should have base property
    assertEquals( "base.value", derived1.get( "base.property" ) );
    assertEquals( "base.value", derived2.get( "base.property" ) );
  }

  @Test
  public void testGetFileSystemWithRegisteredDefaults() throws IOException {
    FileSystemRegistry.registerDefaults();

    // Verify we can get filesystem with a registered scheme
    Configuration conf = FileSystemUtils.createConfiguration();

    // Check that PVFS is registered
    assertEquals( "org.pentaho.hadoop.shim.common.pvfs.PvfsHadoopBridge",
      conf.get( "fs.pvfs.impl" ) );

    // Check that ADL is registered
    assertEquals( "org.apache.hadoop.fs.adl.AdlFileSystem",
      conf.get( "fs.adl.impl" ) );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetFileSystem_InvalidURI() throws IOException {
    // This should throw IllegalArgumentException due to invalid URI format
    // URI.create() throws IllegalArgumentException for malformed URIs
    FileSystemUtils.getFileSystem( "not a valid uri with spaces" );
  }

  @Test
  public void testFileSystemUtilsStaticInitialization() {
    // After @Before clears the registry, test that registerDefaults can be called
    // and properly initializes the registry

    // Initially should be empty after @Before
    assertFalse( "Registry should be clear after @Before",
      FileSystemRegistry.isRegistered( "pvfs" ) );

    // Call registerDefaults
    FileSystemRegistry.registerDefaults();

    // Now defaults should be available
    assertTrue( "Defaults should be registered after calling registerDefaults",
      FileSystemRegistry.isRegistered( "pvfs" ) );
    assertTrue( "Defaults should be registered after calling registerDefaults",
      FileSystemRegistry.isRegistered( "adl" ) );
  }

  @Test
  public void testGetFileSystemWithAbstractFileSystem() throws IOException {
    FileSystemRegistry.registerFileSystem( "file", "org.apache.hadoop.fs.LocalFileSystem" );
    FileSystemRegistry.registerAbstractFileSystem( "file", "org.apache.hadoop.fs.local.LocalFs" );

    Configuration conf = new Configuration();
    FileSystem fs = FileSystemUtils.getFileSystem( "file:///tmp", conf );

    assertNotNull( "FileSystem should not be null", fs );

    // Verify both filesystem and abstract filesystem are in config
    assertEquals( "org.apache.hadoop.fs.LocalFileSystem", conf.get( "fs.file.impl" ) );
    assertEquals( "org.apache.hadoop.fs.local.LocalFs", conf.get( "fs.AbstractFileSystem.file.impl" ) );

    fs.close();
  }
}

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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

/**
 * Unit tests for FileSystemRegistry.
 */
public class FileSystemRegistryTest {

  @Before
  public void setUp() {
    // Clear registry before each test to ensure clean state
    FileSystemRegistry.clearAll();
  }

  @After
  public void tearDown() {
    // Restore defaults after each test
    FileSystemRegistry.registerDefaults();
  }

  @Test
  public void testRegisterFileSystem() {
    FileSystemRegistry.registerFileSystem( "test", "com.example.TestFileSystem" );

    assertTrue( "FileSystem should be registered", FileSystemRegistry.isRegistered( "test" ) );
    assertEquals( "com.example.TestFileSystem", FileSystemRegistry.getImplementation( "test" ) );
  }

  @Test
  public void testRegisterAbstractFileSystem() {
    FileSystemRegistry.registerAbstractFileSystem( "test", "com.example.TestAbstractFileSystem" );

    // Abstract filesystems don't show up in isRegistered (which only checks FILE_SYSTEM_IMPLEMENTATIONS)
    // but they should be applied to configurations
    Configuration conf = new Configuration();
    FileSystemRegistry.applyToConfiguration( conf );

    assertEquals( "com.example.TestAbstractFileSystem",
      conf.get( "fs.AbstractFileSystem.test.impl" ) );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRegisterFileSystem_NullScheme() {
    FileSystemRegistry.registerFileSystem( null, "com.example.TestFileSystem" );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRegisterFileSystem_NullImplementation() {
    FileSystemRegistry.registerFileSystem( "test", null );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRegisterAbstractFileSystem_NullScheme() {
    FileSystemRegistry.registerAbstractFileSystem( null, "com.example.TestAbstractFileSystem" );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRegisterAbstractFileSystem_NullImplementation() {
    FileSystemRegistry.registerAbstractFileSystem( "test", null );
  }

  @Test
  public void testRegisterDefaults() {
    FileSystemRegistry.registerDefaults();

    // Should register PVFS
    assertTrue( "PVFS should be registered", FileSystemRegistry.isRegistered( "pvfs" ) );
    assertEquals( "org.pentaho.hadoop.shim.common.pvfs.PvfsHadoopBridge",
      FileSystemRegistry.getImplementation( "pvfs" ) );

    // Should register Azure Data Lake Gen1
    assertTrue( "ADL should be registered", FileSystemRegistry.isRegistered( "adl" ) );
    assertEquals( "org.apache.hadoop.fs.adl.AdlFileSystem",
      FileSystemRegistry.getImplementation( "adl" ) );

    // Should register Azure Blob Storage
    assertTrue( "WASB should be registered", FileSystemRegistry.isRegistered( "wasb" ) );
    assertTrue( "ABFSS should be registered", FileSystemRegistry.isRegistered( "abfss" ) );

    // Multiple calls should be idempotent
    int firstSize = FileSystemRegistry.getAllRegistrations().size();
    FileSystemRegistry.registerDefaults();
    int secondSize = FileSystemRegistry.getAllRegistrations().size();
    assertEquals( "Multiple registerDefaults calls should be idempotent", firstSize, secondSize );
  }

  @Test
  public void testUnregisterFileSystem() {
    FileSystemRegistry.registerFileSystem( "test", "com.example.TestFileSystem" );
    assertTrue( "FileSystem should be registered", FileSystemRegistry.isRegistered( "test" ) );

    FileSystemRegistry.unregisterFileSystem( "test" );
    assertFalse( "FileSystem should be unregistered", FileSystemRegistry.isRegistered( "test" ) );
    assertNull( "Implementation should be null", FileSystemRegistry.getImplementation( "test" ) );
  }

  @Test
  public void testClearAll() {
    FileSystemRegistry.registerDefaults();
    assertTrue( "Should have registrations", FileSystemRegistry.getAllRegistrations().size() > 0 );

    FileSystemRegistry.clearAll();
    assertEquals( "All registrations should be cleared", 0, FileSystemRegistry.getAllRegistrations().size() );
    assertFalse( "PVFS should not be registered", FileSystemRegistry.isRegistered( "pvfs" ) );
  }

  @Test
  public void testApplyToConfiguration() {
    FileSystemRegistry.registerFileSystem( "test", "com.example.TestFileSystem" );
    FileSystemRegistry.registerAbstractFileSystem( "test", "com.example.TestAbstractFileSystem" );

    Configuration conf = new Configuration();
    FileSystemRegistry.applyToConfiguration( conf );

    assertEquals( "com.example.TestFileSystem", conf.get( "fs.test.impl" ) );
    assertEquals( "com.example.TestAbstractFileSystem", conf.get( "fs.AbstractFileSystem.test.impl" ) );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testApplyToConfiguration_NullConfiguration() {
    FileSystemRegistry.applyToConfiguration( null );
  }

  @Test
  public void testApplyToConfiguration_WithDefaults() {
    FileSystemRegistry.registerDefaults();

    Configuration conf = new Configuration();
    FileSystemRegistry.applyToConfiguration( conf );

    // Verify PVFS is applied
    assertEquals( "org.pentaho.hadoop.shim.common.pvfs.PvfsHadoopBridge",
      conf.get( "fs.pvfs.impl" ) );

    // Verify ADL is applied (both FileSystem and AbstractFileSystem)
    assertEquals( "org.apache.hadoop.fs.adl.AdlFileSystem",
      conf.get( "fs.adl.impl" ) );
    assertEquals( "org.apache.hadoop.fs.adl.Adl",
      conf.get( "fs.AbstractFileSystem.adl.impl" ) );

    // Verify Azure Blob Storage is applied
    assertEquals( "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem",
      conf.get( "fs.abfss.impl" ) );
    assertEquals( "org.apache.hadoop.fs.azurebfs.Abfss",
      conf.get( "fs.AbstractFileSystem.abfss.impl" ) );
  }

  @Test
  public void testCreateConfiguration() {
    FileSystemRegistry.registerFileSystem( "test", "com.example.TestFileSystem" );

    Configuration conf = FileSystemRegistry.createConfiguration();

    assertNotNull( "Configuration should not be null", conf );
    assertEquals( "com.example.TestFileSystem", conf.get( "fs.test.impl" ) );
  }

  @Test
  public void testCreateConfiguration_WithBase() {
    Configuration base = new Configuration();
    base.set( "custom.property", "custom.value" );

    FileSystemRegistry.registerFileSystem( "test", "com.example.TestFileSystem" );

    Configuration conf = FileSystemRegistry.createConfiguration( base );

    assertNotNull( "Configuration should not be null", conf );
    assertEquals( "Should inherit base property", "custom.value", conf.get( "custom.property" ) );
    assertEquals( "Should have registered filesystem", "com.example.TestFileSystem", conf.get( "fs.test.impl" ) );
  }

  @Test
  public void testGetImplementation() {
    FileSystemRegistry.registerFileSystem( "test", "com.example.TestFileSystem" );

    assertEquals( "com.example.TestFileSystem", FileSystemRegistry.getImplementation( "test" ) );
    assertNull( "Non-existent scheme should return null", FileSystemRegistry.getImplementation( "nonexistent" ) );
  }

  @Test
  public void testIsRegistered() {
    assertFalse( "Scheme should not be registered initially", FileSystemRegistry.isRegistered( "test" ) );

    FileSystemRegistry.registerFileSystem( "test", "com.example.TestFileSystem" );
    assertTrue( "Scheme should be registered", FileSystemRegistry.isRegistered( "test" ) );
  }

  @Test
  public void testGetAllRegistrations() {
    FileSystemRegistry.registerFileSystem( "test1", "com.example.TestFileSystem1" );
    FileSystemRegistry.registerFileSystem( "test2", "com.example.TestFileSystem2" );

    Map<String, String> registrations = FileSystemRegistry.getAllRegistrations();

    assertNotNull( "Registrations should not be null", registrations );
    assertEquals( 2, registrations.size() );
    assertEquals( "com.example.TestFileSystem1", registrations.get( "test1" ) );
    assertEquals( "com.example.TestFileSystem2", registrations.get( "test2" ) );

    // Modifying returned map should not affect registry
    registrations.put( "test3", "com.example.TestFileSystem3" );
    assertFalse( "Registry should not be affected by modifications to returned map",
      FileSystemRegistry.isRegistered( "test3" ) );
  }

  @Test
  public void testRegisterFileSystemIfAvailable_Available() {
    // org.apache.hadoop.conf.Configuration should be available
    FileSystemRegistry.registerFileSystemIfAvailable( "test", "org.apache.hadoop.conf.Configuration" );

    assertTrue( "FileSystem should be registered when class is available",
      FileSystemRegistry.isRegistered( "test" ) );
  }

  @Test
  public void testRegisterFileSystemIfAvailable_NotAvailable() {
    // This class should not exist
    FileSystemRegistry.registerFileSystemIfAvailable( "test", "com.example.NonExistentClass" );

    assertFalse( "FileSystem should not be registered when class is not available",
      FileSystemRegistry.isRegistered( "test" ) );
  }

  @Test
  public void testRegisterAbstractFileSystemIfAvailable_Available() {
    // org.apache.hadoop.conf.Configuration should be available
    FileSystemRegistry.registerAbstractFileSystemIfAvailable( "test", "org.apache.hadoop.conf.Configuration" );

    Configuration conf = new Configuration();
    FileSystemRegistry.applyToConfiguration( conf );

    assertNotNull( "AbstractFileSystem should be registered when class is available",
      conf.get( "fs.AbstractFileSystem.test.impl" ) );
  }

  @Test
  public void testRegisterAbstractFileSystemIfAvailable_NotAvailable() {
    // This class should not exist
    FileSystemRegistry.registerAbstractFileSystemIfAvailable( "test", "com.example.NonExistentClass" );

    Configuration conf = new Configuration();
    FileSystemRegistry.applyToConfiguration( conf );

    assertNull( "AbstractFileSystem should not be registered when class is not available",
      conf.get( "fs.AbstractFileSystem.test.impl" ) );
  }

  @Test
  public void testOverrideExistingRegistration() {
    FileSystemRegistry.registerFileSystem( "test", "com.example.TestFileSystem1" );
    assertEquals( "com.example.TestFileSystem1", FileSystemRegistry.getImplementation( "test" ) );

    // Override with new implementation
    FileSystemRegistry.registerFileSystem( "test", "com.example.TestFileSystem2" );
    assertEquals( "Should use new implementation", "com.example.TestFileSystem2",
      FileSystemRegistry.getImplementation( "test" ) );
  }

  @Test
  public void testMultipleSchemes() {
    FileSystemRegistry.registerFileSystem( "scheme1", "com.example.FileSystem1" );
    FileSystemRegistry.registerFileSystem( "scheme2", "com.example.FileSystem2" );
    FileSystemRegistry.registerFileSystem( "scheme3", "com.example.FileSystem3" );

    Configuration conf = new Configuration();
    FileSystemRegistry.applyToConfiguration( conf );

    assertEquals( "com.example.FileSystem1", conf.get( "fs.scheme1.impl" ) );
    assertEquals( "com.example.FileSystem2", conf.get( "fs.scheme2.impl" ) );
    assertEquals( "com.example.FileSystem3", conf.get( "fs.scheme3.impl" ) );
  }

  @Test
  public void testConfigurationNotModifiedDirectly() {
    Configuration conf1 = new Configuration();
    conf1.set( "test.property", "test.value" );

    FileSystemRegistry.registerFileSystem( "test", "com.example.TestFileSystem" );
    FileSystemRegistry.applyToConfiguration( conf1 );

    // Create another configuration - should not have registrations automatically
    Configuration conf2 = new Configuration();
    assertNull( "New configuration should not have auto-applied registrations",
      conf2.get( "fs.test.impl" ) );

    // But after applying, it should
    FileSystemRegistry.applyToConfiguration( conf2 );
    assertEquals( "com.example.TestFileSystem", conf2.get( "fs.test.impl" ) );
  }
}

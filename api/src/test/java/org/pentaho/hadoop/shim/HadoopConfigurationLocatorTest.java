/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.hadoop.shim;

import org.apache.commons.vfs2.AllFileSelector;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.exporter.ZipExporter;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.hadoop.shim.api.ShimProperties;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.pentaho.hadoop.shim.spi.MockHadoopShim;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;

public class HadoopConfigurationLocatorTest {

  private static String HADOOP_CONFIGURATIONS_PATH = System.getProperty( "java.io.tmpdir" ) + "/hadoop-configurations";
  private static FileObject configFile;

  @BeforeClass
  public static void setup() throws Exception {
    // Create a test hadoop configuration "a"
    FileObject ramRoot = VFS.getManager().resolveFile( HADOOP_CONFIGURATIONS_PATH );
    FileObject aConfigFolder = ramRoot.resolveFile( "a" );
    if ( aConfigFolder.exists() ) {
      aConfigFolder.delete( new AllFileSelector() );
    }
    aConfigFolder.createFolder();

    assertEquals( FileType.FOLDER, aConfigFolder.getType() );

    // Create the properties file for the configuration as hadoop-configurations/a/config.properties
    configFile = aConfigFolder.resolveFile( "config.properties" );
    Properties p = new Properties();
    p.setProperty( "name", "Test Configuration A" );
    p.setProperty( "classpath", "" );
    p.setProperty( "ignore.classes", "" );
    p.setProperty( "library.path", "" );
    p.setProperty( "required.classes", HadoopConfigurationLocatorTest.class.getName() );
    p.store( configFile.getContent().getOutputStream(), "Test Configuration A" );
    configFile.close();

    // Create the implementation jar
    FileObject implJar = aConfigFolder.resolveFile( "a-config.jar" );
    implJar.createFile();

    // Use ShrinkWrap to create the jar and write it out to VFS
    JavaArchive archive = ShrinkWrap.create( JavaArchive.class, "a-configuration.jar" ).addAsServiceProvider(
      HadoopShim.class, MockHadoopShim.class )
      .addClass( MockHadoopShim.class );
    archive.as( ZipExporter.class ).exportTo( implJar.getContent().getOutputStream() );
  }

  @Test( expected = ConfigurationException.class )
  public void init_invalidDirectory() throws FileSystemException, ConfigurationException {
    HadoopConfigurationLocator locator = new HadoopConfigurationLocator();
    locator.init( VFS.getManager().resolveFile( "ram://bogus-path" ), new MockActiveHadoopConfigurationLocator(),
      new DefaultFileSystemManager() );
  }

  @Test( expected = NullPointerException.class )
  public void init_null_basedir() throws FileSystemException, ConfigurationException {
    HadoopConfigurationLocator locator = new HadoopConfigurationLocator();
    locator.init( null, new MockActiveHadoopConfigurationLocator(),
      new DefaultFileSystemManager() );
  }

  @Test( expected = NullPointerException.class )
  public void init_null_activeLocator() throws FileSystemException, ConfigurationException {
    HadoopConfigurationLocator locator = new HadoopConfigurationLocator();
    locator.init( VFS.getManager().resolveFile( HADOOP_CONFIGURATIONS_PATH ), null,
      new DefaultFileSystemManager() );
  }

  @Test( expected = NullPointerException.class )
  public void init_null_fsm() throws FileSystemException, ConfigurationException {
    HadoopConfigurationLocator locator = new HadoopConfigurationLocator();
    locator.init( VFS.getManager().resolveFile( HADOOP_CONFIGURATIONS_PATH ),
      new MockActiveHadoopConfigurationLocator(),
      null );
  }

  @Test
  public void init_MissingRequiredClasses() throws IOException {
    Properties properties = new Properties();
    InputStream inputStream = configFile.getContent().getInputStream();
    properties.load( inputStream );
    inputStream.close();
    properties.setProperty( "required.classes", "this.class.does.not.Exist" );
    properties.store( configFile.getContent().getOutputStream(), "Test Configuration A" );
    configFile.close();
    HadoopConfigurationLocator locator = new HadoopConfigurationLocator();
    try {
      locator.init(
        VFS.getManager().resolveFile( HADOOP_CONFIGURATIONS_PATH ), new MockActiveHadoopConfigurationLocator( "a" ),
        new DefaultFileSystemManager() );
      Assert.fail( "Should have got exception " );
    } catch ( ConfigurationException e ) {
      assertEquals(
        "Unable to load class this.class.does.not.Exist that is required to start the Test Configuration A Hadoop Shim",
        e.getCause().getMessage() );
    } finally {
      properties.setProperty( "required.classes", HadoopConfigurationLocator.class.getName() );
      properties.store( configFile.getContent().getOutputStream(), "Test Configuration A" );
      configFile.close();
    }
  }

  public void init() throws FileSystemException, ConfigurationException {
    HadoopConfigurationLocator locator = new HadoopConfigurationLocator();
    locator.init( VFS.getManager().resolveFile( HADOOP_CONFIGURATIONS_PATH ),
      new MockActiveHadoopConfigurationLocator( "a" ),
      new DefaultFileSystemManager() );

    assertEquals( 1, locator.getConfigurations().size() );
    assertEquals( VFS.getManager().resolveFile( HADOOP_CONFIGURATIONS_PATH ).resolveFile( "a" ),
      locator.getConfiguration( "a" ).getLocation() );
  }

  @Test
  public void hasConfiguration() throws Exception {
    HadoopConfigurationLocator locator = new HadoopConfigurationLocator();
    locator.init( VFS.getManager().resolveFile( HADOOP_CONFIGURATIONS_PATH ),
      new MockActiveHadoopConfigurationLocator( "a" ),
      new DefaultFileSystemManager() );

    assertTrue( locator.hasConfiguration( "a" ) );
  }

  @Test( expected = RuntimeException.class )
  public void hasConfiguration_not_intialized() {
    HadoopConfigurationLocator locator = new HadoopConfigurationLocator();
    locator.hasConfiguration( null );
  }

  @Test
  public void getConfiguration() throws Exception {
    HadoopConfigurationLocator locator = new HadoopConfigurationLocator();
    locator.init( VFS.getManager().resolveFile( HADOOP_CONFIGURATIONS_PATH ),
      new MockActiveHadoopConfigurationLocator( "a" ),
      new DefaultFileSystemManager() );

    HadoopConfiguration a = locator.getConfiguration( "a" );
    assertNotNull( a );
    assertEquals( "a", a.getIdentifier() );
    assertEquals( "Test Configuration A", a.getName() );
  }

  @Test( expected = ConfigurationException.class )
  public void getConfiguration_unknown_id() throws Exception {
    HadoopConfigurationLocator locator = new HadoopConfigurationLocator();
    locator.init( VFS.getManager().resolveFile( HADOOP_CONFIGURATIONS_PATH ),
      new MockActiveHadoopConfigurationLocator(),
      new DefaultFileSystemManager() );

    locator.getConfiguration( "unknown" );
  }

  @Test( expected = RuntimeException.class )
  public void getConfiguration_not_intialized() throws ConfigurationException {
    HadoopConfigurationLocator locator = new HadoopConfigurationLocator();
    locator.getConfiguration( null );
  }

  @Test( expected = RuntimeException.class )
  public void getConfigurations_not_intialized() {
    HadoopConfigurationLocator locator = new HadoopConfigurationLocator();
    locator.getConfigurations();
  }

  @Test
  public void getActiveConfiguration() throws Exception {
    HadoopConfigurationLocator locator = new HadoopConfigurationLocator();
    locator.init( VFS.getManager().resolveFile( HADOOP_CONFIGURATIONS_PATH ),
      new MockActiveHadoopConfigurationLocator( "a" ),
      new DefaultFileSystemManager() );

    HadoopConfiguration a = locator.getActiveConfiguration();
    assertNotNull( a );
  }


  @Test( expected = RuntimeException.class )
  public void getActiveConfiguration_not_intialized() throws ConfigurationException {
    HadoopConfigurationLocator locator = new HadoopConfigurationLocator();
    locator.getActiveConfiguration();
  }

  @Test( expected = NullPointerException.class )
  public void registerNativeLibraryPath_null_path() throws SecurityException, NoSuchFieldException,
    IllegalArgumentException, IllegalAccessException {
    HadoopConfigurationLocator locator = new HadoopConfigurationLocator();
    locator.registerNativeLibraryPath( null );
  }

  @Test
  public void registerNativeLibraryPaths() throws SecurityException, NoSuchFieldException, IllegalArgumentException,
    IllegalAccessException {
    HadoopConfigurationLocator locator = new HadoopConfigurationLocator();
    locator.registerNativeLibraryPaths( "test,ing" );

    Field f = ClassLoader.class.getDeclaredField( "usr_paths" );
    boolean accessible = f.isAccessible();
    f.setAccessible( true );
    try {
      String[] usrPaths = (String[]) f.get( null );
      assertTrue( Arrays.asList( usrPaths ).contains( "test" ) );
      assertTrue( Arrays.asList( usrPaths ).contains( "ing" ) );
    } finally {
      f.setAccessible( accessible );
    }
  }

  @Test
  public void registerNativeLibraryPaths_no_duplicates() throws SecurityException, NoSuchFieldException,
    IllegalArgumentException, IllegalAccessException {
    HadoopConfigurationLocator locator = new HadoopConfigurationLocator();
    locator.registerNativeLibraryPaths( "test,ing" );

    Field f = ClassLoader.class.getDeclaredField( "usr_paths" );
    boolean accessible = f.isAccessible();
    f.setAccessible( true );
    try {
      String[] usrPaths = (String[]) f.get( null );
      int pathCount = usrPaths.length;
      locator.registerNativeLibraryPaths( "ing" );
      usrPaths = (String[]) f.get( null );
      assertTrue( Arrays.asList( usrPaths ).contains( "test" ) );
      assertTrue( Arrays.asList( usrPaths ).contains( "ing" ) );
      assertEquals( pathCount, usrPaths.length );
    } finally {
      f.setAccessible( accessible );
    }
  }

  @Test( expected = ConfigurationException.class )
  public void createConfigurationLoader_null_root() throws ConfigurationException {
    HadoopConfigurationLocator locator = new HadoopConfigurationLocator();
    locator.createConfigurationLoader( null, null, null, new ShimProperties() );
  }

  @Test( expected = ConfigurationException.class )
  public void createConfigurationLoader_root_not_a_folder() throws Exception {
    HadoopConfigurationLocator locator = new HadoopConfigurationLocator();
    // Try to create a configuration based on a file, not a folder
    FileObject buildProperties = VFS.getManager().resolveFile( "ram:///test.file" );
    buildProperties.createFile();
    assertEquals( FileType.FILE, buildProperties.getType() );
    locator.createConfigurationLoader( buildProperties, null, null, new ShimProperties() );
  }

  @Test
  public void loadHadoopConfiguration_with_ignore_classes() throws Exception {
    HadoopConfigurationLocator locator = new HadoopConfigurationLocator();

    FileObject root = VFS.getManager().resolveFile( HADOOP_CONFIGURATIONS_PATH + "/a" );
    HadoopConfiguration configuration = locator.loadHadoopConfiguration( root );

    assertNotNull( configuration );
  }

  @Test
  public void createConfigurationLoader() throws Exception {
    HadoopConfigurationLocator locator = new HadoopConfigurationLocator();

    FileObject root = VFS.getManager().resolveFile( HADOOP_CONFIGURATIONS_PATH + "/a" );
    ClassLoader cl = locator.createConfigurationLoader( root, getClass().getClassLoader(), null, new ShimProperties() );

    assertNotNull( cl.getResource( "config.properties" ) );
  }

  @Test( expected = ConfigurationException.class )
  public void findHadoopConfigurations_errorLoadingHadoopConfig() throws Exception {
    FileObject root = VFS.getManager().resolveFile( HADOOP_CONFIGURATIONS_PATH );
    HadoopConfigurationLocator locator = new HadoopConfigurationLocator() {
      protected HadoopConfiguration loadHadoopConfiguration( FileObject folder ) throws ConfigurationException {
        throw new ConfigurationException( "test" );
      }
    };
    locator.init( root, new MockActiveHadoopConfigurationLocator( "a" ), new DefaultFileSystemManager() );
  }

  @Test
  public void parseURLs() throws Exception {
    HadoopConfigurationLocator locator = new HadoopConfigurationLocator();
    FileObject root = VFS.getManager().resolveFile( HADOOP_CONFIGURATIONS_PATH );

    List<URL> urls = locator.parseURLs( root, "a,b" );
    assertEquals( 2, urls.size() );
    assertEquals( root.getURL().toURI().resolve( "hadoop-configurations/a/" ), urls.get( 0 ).toURI() );
    assertEquals( root.getURL().toURI().resolve( "hadoop-configurations/a/a-config.jar" ), urls.get( 1 ).toURI() );
  }
}

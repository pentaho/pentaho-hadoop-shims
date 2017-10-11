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
package org.pentaho.hadoop.shim.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.vfs2.AllFileSelector;
import org.apache.commons.vfs2.FileObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.spi.MockHadoopShim;

/**
 * There are tests of DistributedCacheUtil using hadoop local file system implementation. So these tests requires
 * additional settings to be run on Windows: it needs to have <b>hadoop.home.dir</b> variable pointed to dir with
 * <i>\bin\winutils.exe</i>
 * <p>
 * Depending on possible issues with hadoop file system on Windows any of these tests can be skipped. E.g. using the
 * following code below:
 *
 * <pre>
 * <code>
 * // Don't run this test on Windows env
 * assumeTrue( !isWindows() );
 * </code>
 * </pre>
 */
public class DistributedCacheUtilImplOSDependentTest {
  private static HadoopConfiguration TEST_CONFIG;
  private static String PLUGIN_BASE = null;
  private static final String OS_NAME = System.getProperty( "os.name", "unknown" );

  protected static boolean isWindows() {
    return OS_NAME.startsWith( "Windows" );
  }

  @BeforeClass
  public static void setup() throws Exception {
    // Create some Hadoop configuration specific pmr libraries
    TEST_CONFIG = new HadoopConfiguration( DistributedCacheTestUtil.createTestHadoopConfiguration( "bin/test/" + DistributedCacheUtilImplOSDependentTest.class.getSimpleName() ), "test-config", "name", new MockHadoopShim() );

    PLUGIN_BASE = System.getProperty( Const.PLUGIN_BASE_FOLDERS_PROP );
    // Fake out the "plugins" directory for the project's root directory
    System.setProperty( Const.PLUGIN_BASE_FOLDERS_PROP, KettleVFS.getFileObject( "." ).getURL().toURI().getPath() );
  }

  @AfterClass
  public static void teardown() {
    if ( PLUGIN_BASE != null ) {
      System.setProperty( Const.PLUGIN_BASE_FOLDERS_PROP, PLUGIN_BASE );
    }
  }

  @Test
  public void stageForCache() throws Exception {
    DistributedCacheUtilImpl ch = new DistributedCacheUtilImpl( TEST_CONFIG );

    // Copy the contents of test folder
    FileObject source = DistributedCacheTestUtil.createTestFolderWithContent();

    try {
      Path root = new Path( "bin/test/stageArchiveForCacheTest" );
      Path dest = new Path( root, "org/pentaho/mapreduce/" );

      Configuration conf = new Configuration();
      FileSystem fs = DistributedCacheTestUtil.getLocalFileSystem( conf );

      DistributedCacheTestUtil.stageForCacheTester( ch, source, fs, root, dest, 6, 6 );
    } finally {
      source.delete( new AllFileSelector() );
    }
  }

  @Test
  public void stageForCache_destination_exists() throws Exception {
    DistributedCacheUtilImpl ch = new DistributedCacheUtilImpl( TEST_CONFIG );

    Configuration conf = new Configuration();
    FileSystem fs = DistributedCacheTestUtil.getLocalFileSystem( conf );

    FileObject source = DistributedCacheTestUtil.createTestFolderWithContent();
    try {
      Path root = new Path( "bin/test/stageForCache_destination_exists" );
      Path dest = new Path( root, "dest" );

      fs.mkdirs( dest );
      assertTrue( fs.exists( dest ) );
      assertTrue( fs.getFileStatus( dest ).isDir() );

      DistributedCacheTestUtil.stageForCacheTester( ch, source, fs, root, dest, 6, 6 );
    } finally {
      source.delete( new AllFileSelector() );
    }
  }

  @Test
  public void stagePluginsForCache() throws Exception {
    DistributedCacheUtilImpl ch = new DistributedCacheUtilImpl( TEST_CONFIG );

    Configuration conf = new Configuration();
    FileSystem fs = DistributedCacheTestUtil.getLocalFileSystem( conf );

    Path pluginsDir = new Path( "bin/test/plugins-installation-dir" );

    FileObject pluginDir = DistributedCacheTestUtil.createTestFolderWithContent();

    try {
      ch.stagePluginsForCache( fs, pluginsDir, "bin/test/sample-folder" );
      Path pluginInstallPath = new Path( pluginsDir, "bin/test/sample-folder" );
      assertTrue( fs.exists( pluginInstallPath ) );
      ContentSummary summary = fs.getContentSummary( pluginInstallPath );
      assertEquals( 6, summary.getFileCount() );
      assertEquals( 6, summary.getDirectoryCount() );
    } finally {
      pluginDir.delete( new AllFileSelector() );
      fs.delete( pluginsDir, true );
    }
  }

  @Test
  public void findFiles_hdfs_native() throws Exception {
    DistributedCacheUtilImpl ch = new DistributedCacheUtilImpl( TEST_CONFIG );

    // Copy the contents of test folder
    FileObject source = DistributedCacheTestUtil.createTestFolderWithContent();
    Path root = new Path( "bin/test/stageArchiveForCacheTest" );
    Configuration conf = new Configuration();
    FileSystem fs = DistributedCacheTestUtil.getLocalFileSystem( conf );
    Path dest = new Path( root, "org/pentaho/mapreduce/" );
    try {
      try {
        ch.stageForCache( source, fs, dest, true );

        List<Path> files = ch.findFiles( fs, dest, null );
        assertEquals( 5, files.size() );

        files = ch.findFiles( fs, dest, Pattern.compile( ".*jar$" ) );
        assertEquals( 2, files.size() );

        files = ch.findFiles( fs, dest, Pattern.compile( ".*folder$" ) );
        assertEquals( 1, files.size() );
      } finally {
        fs.delete( root, true );
      }
    } finally {
      source.delete( new AllFileSelector() );
    }
  }

  @Test
  public void installKettleEnvironment() throws Exception {
    DistributedCacheUtilImpl ch = new DistributedCacheUtilImpl( TEST_CONFIG );

    Configuration conf = new Configuration();
    FileSystem fs = DistributedCacheTestUtil.getLocalFileSystem( conf );

    // This "empty pmr" contains a lib/ folder but with no content
    FileObject pmrArchive = KettleVFS.getFileObject( getClass().getResource( "/empty-pmr.zip" ).toURI().getPath() );

    FileObject bigDataPluginDir = DistributedCacheTestUtil.createTestFolderWithContent( DistributedCacheUtilImpl.PENTAHO_BIG_DATA_PLUGIN_FOLDER_NAME );

    Path root = new Path( "bin/test/installKettleEnvironment" );
    try {
      ch.installKettleEnvironment( pmrArchive, fs, root, bigDataPluginDir, null );
      assertTrue( ch.isKettleEnvironmentInstalledAt( fs, root ) );
    } finally {
      bigDataPluginDir.delete( new AllFileSelector() );
      fs.delete( root, true );
    }
  }

  @Test
  public void installKettleEnvironment_additional_plugins() throws Exception {
    DistributedCacheUtilImpl ch = new DistributedCacheUtilImpl( TEST_CONFIG );

    Configuration conf = new Configuration();
    FileSystem fs = DistributedCacheTestUtil.getLocalFileSystem( conf );

    // This "empty pmr" contains a lib/ folder but with no content
    FileObject pmrArchive = KettleVFS.getFileObject( getClass().getResource( "/empty-pmr.zip" ).toURI().getPath() );
    FileObject bigDataPluginDir = DistributedCacheTestUtil.createTestFolderWithContent( DistributedCacheUtilImpl.PENTAHO_BIG_DATA_PLUGIN_FOLDER_NAME );

    String pluginName = "additional-plugin";
    FileObject additionalPluginDir = DistributedCacheTestUtil.createTestFolderWithContent( pluginName );
    Path root = new Path( "bin/test/installKettleEnvironment" );
    try {
      ch.installKettleEnvironment( pmrArchive, fs, root, bigDataPluginDir, "bin/test/" + pluginName );
      assertTrue( ch.isKettleEnvironmentInstalledAt( fs, root ) );
      assertTrue( fs.exists( new Path( root, "plugins/bin/test/" + pluginName ) ) );
    } finally {
      bigDataPluginDir.delete( new AllFileSelector() );
      additionalPluginDir.delete( new AllFileSelector() );
      fs.delete( root, true );
    }
  }

  @Test
  public void isPmrInstalledAt() throws IOException {
    DistributedCacheUtilImpl ch = new DistributedCacheUtilImpl( TEST_CONFIG );

    Configuration conf = new Configuration();
    FileSystem fs = DistributedCacheTestUtil.getLocalFileSystem( conf );

    Path root = new Path( "bin/test/ispmrInstalledAt" );
    Path lib = new Path( root, "lib" );
    Path plugins = new Path( root, "plugins" );
    Path bigDataPlugin = new Path( plugins, DistributedCacheUtilImpl.PENTAHO_BIG_DATA_PLUGIN_FOLDER_NAME );

    Path lockFile = ch.getLockFileAt( root );
    FSDataOutputStream lockFileOut = null;
    FSDataOutputStream bigDataPluginFileOut = null;
    try {
      // Create all directories (parent directories created automatically)
      fs.mkdirs( lib );
      fs.mkdirs( bigDataPlugin );

      assertTrue( ch.isKettleEnvironmentInstalledAt( fs, root ) );

      // If lock file is there pmr is not installed
      lockFileOut = fs.create( lockFile );
      assertFalse( ch.isKettleEnvironmentInstalledAt( fs, root ) );

      // Try to create a file instead of a directory for the pentaho-big-data-plugin. This should be detected.
      fs.delete( bigDataPlugin, true );
      bigDataPluginFileOut = fs.create( bigDataPlugin );
      assertFalse( ch.isKettleEnvironmentInstalledAt( fs, root ) );
    } finally {
      lockFileOut.close();
      bigDataPluginFileOut.close();
      fs.delete( root, true );
    }
  }

  @Test
  public void configureWithPmr() throws Exception {
    DistributedCacheUtilImpl ch = new DistributedCacheUtilImpl( TEST_CONFIG );

    Configuration conf = new Configuration();
    FileSystem fs = DistributedCacheTestUtil.getLocalFileSystem( conf );

    // This "empty pmr" contains a lib/ folder and some empty kettle-*.jar files but no actual content
    FileObject pmrArchive = KettleVFS.getFileObject( getClass().getResource( "/empty-pmr.zip" ).toURI().getPath() );

    FileObject bigDataPluginDir = DistributedCacheTestUtil.createTestFolderWithContent( DistributedCacheUtilImpl.PENTAHO_BIG_DATA_PLUGIN_FOLDER_NAME );

    Path root = new Path( "bin/test/installKettleEnvironment" );
    try {
      ch.installKettleEnvironment( pmrArchive, fs, root, bigDataPluginDir, null );
      assertTrue( ch.isKettleEnvironmentInstalledAt( fs, root ) );

      ch.configureWithKettleEnvironment( conf, fs, root );

      // Make sure our libraries are on the classpathi
      assertTrue( conf.get( "mapred.cache.files" ).contains( "lib/kettle-core.jar" ) );
      assertTrue( conf.get( "mapred.cache.files" ).contains( "lib/kettle-engine.jar" ) );
      assertTrue( conf.get( "mapred.job.classpath.files" ).contains( "lib/kettle-core.jar" ) );
      assertTrue( conf.get( "mapred.job.classpath.files" ).contains( "lib/kettle-engine.jar" ) );

      // Make sure the configuration specific jar made it!
      assertTrue( conf.get( "mapred.cache.files" ).contains( "lib/configuration-specific.jar" ) );

      // Make sure our plugins folder is registered
      assertTrue( conf.get( "mapred.cache.files" ).contains( "#plugins" ) );

      // Make sure our libraries aren't included twice
      assertFalse( conf.get( "mapred.cache.files" ).contains( "#lib" ) );

      // We should not have individual files registered
      assertFalse( conf.get( "mapred.cache.files" ).contains( "pentaho-big-data-plugin/jar1.jar" ) );
      assertFalse( conf.get( "mapred.cache.files" ).contains( "pentaho-big-data-plugin/jar2.jar" ) );
      assertFalse( conf.get( "mapred.cache.files" ).contains( "pentaho-big-data-plugin/folder/file.txt" ) );

    } finally {
      bigDataPluginDir.delete( new AllFileSelector() );
      fs.delete( root, true );
    }
  }

}

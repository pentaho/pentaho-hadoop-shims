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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyShort;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.vfs2.AllFileSelector;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.common.fs.PathProxy;
import org.pentaho.hadoop.shim.spi.MockHadoopShim;

/**
 * Test the DistributedCacheUtil
 */
public class DistributedCacheUtilImplTest {

  private static HadoopConfiguration TEST_CONFIG;
  private static String PLUGIN_BASE = null;

  @BeforeClass
  public static void setup() throws Exception {
    // Create some Hadoop configuration specific pmr libraries
    TEST_CONFIG = new HadoopConfiguration( DistributedCacheTestUtil.createTestHadoopConfiguration( "bin/test/" + DistributedCacheUtilImplTest.class.getSimpleName() ), "test-config", "name", new MockHadoopShim() );

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

  @Test( expected = NullPointerException.class )
  public void instantiation() {
    new DistributedCacheUtilImpl( null );
  }

  @Test
  public void deleteDirectory() throws Exception {
    FileObject test = KettleVFS.getFileObject( "bin/test/deleteDirectoryTest" );
    test.createFolder();

    DistributedCacheUtilImpl ch = new DistributedCacheUtilImpl( TEST_CONFIG );
    ch.deleteDirectory( test );
    try {
      assertFalse( test.exists() );
    } finally {
      // Delete the directory with java.io.File if it wasn't removed
      File f = new File( "bin/test/deleteDirectoryTest" );
      if ( f.exists() && !f.delete() ) {
        throw new IOException( "unable to delete test directory: " + f.getAbsolutePath() );
      }
    }
  }

  @Test
  public void extract_invalid_archive() throws Exception {
    DistributedCacheUtilImpl ch = new DistributedCacheUtilImpl( TEST_CONFIG );

    try {
      ch.extract( KettleVFS.getFileObject( "bogus" ), null );
      fail( "expected exception" );
    } catch ( IllegalArgumentException ex ) {
      assertTrue( ex.getMessage().startsWith( "archive does not exist" ) );
    }
  }

  @Test
  public void extract_destination_exists() throws Exception {
    DistributedCacheUtilImpl ch = new DistributedCacheUtilImpl( TEST_CONFIG );

    FileObject archive = KettleVFS.getFileObject( getClass().getResource( "/pentaho-mapreduce-sample.jar" ).toURI().getPath() );

    try {
      ch.extract( archive, KettleVFS.getFileObject( "." ) );
    } catch ( IllegalArgumentException ex ) {
      assertTrue( ex.getMessage(), "destination already exists".equals( ex.getMessage() ) );
    }
  }

  @Test
  public void extractToTemp() throws Exception {
    DistributedCacheUtilImpl ch = new DistributedCacheUtilImpl( TEST_CONFIG );

    FileObject archive = KettleVFS.getFileObject( getClass().getResource( "/pentaho-mapreduce-sample.jar" ).toURI().getPath() );
    FileObject extracted = ch.extractToTemp( archive );

    assertNotNull( extracted );
    assertTrue( extracted.exists() );
    try {
      // There should be 3 files and 5 directories inside the root folder (which is the 9th entry)
      assertTrue( extracted.findFiles( new AllFileSelector() ).length == 9 );
    } finally {
      // clean up after ourself
      ch.deleteDirectory( extracted );
    }
  }

  @Test
  public void extractToTempZipEntriesMixed() throws Exception {
    DistributedCacheUtilImpl ch = new DistributedCacheUtilImpl( TEST_CONFIG );

    File dest = File.createTempFile( "entriesMixed", ".zip" );
    ZipOutputStream outputStream = new ZipOutputStream( new FileOutputStream( dest ) );
    ZipEntry e = new ZipEntry( "zipEntriesMixed" + "/" + "someFile.txt" );
    outputStream.putNextEntry( e );
    byte[] data = "someOutString".getBytes();
    outputStream.write( data, 0, data.length );
    outputStream.closeEntry();
    e = new ZipEntry( "zipEntriesMixed" + "/" );
    outputStream.putNextEntry( e );
    outputStream.closeEntry();
    outputStream.close();

    FileObject archive = KettleVFS.getFileObject( dest.getAbsolutePath() );

    FileObject extracted = null;
    try {
      extracted = ch.extractToTemp( archive );
    } catch ( IOException | KettleFileException e1 ) {
      e1.printStackTrace();
      fail( "Exception not expected in this case" );
    }

    assertNotNull( extracted );
    assertTrue( extracted.exists() );
    try {
      // There should be 3 files and 5 directories inside the root folder (which is the 9th entry)
      assertTrue( extracted.findFiles( new AllFileSelector() ).length == 3 );
    } finally {
      // clean up after ourself
      ch.deleteDirectory( extracted );
      dest.delete();
    }
  }

  @Test
  public void extractToTemp_missing_archive() throws Exception {
    DistributedCacheUtilImpl ch = new DistributedCacheUtilImpl( TEST_CONFIG );

    try {
      ch.extractToTemp( null );
      fail( "Expected exception" );
    } catch ( NullPointerException ex ) {
      assertEquals( "archive is required", ex.getMessage() );
    }
  }

  @Test
  public void findFiles_vfs() throws Exception {
    DistributedCacheUtilImpl ch = new DistributedCacheUtilImpl( TEST_CONFIG );

    FileObject testFolder = DistributedCacheTestUtil.createTestFolderWithContent();

    try {
      // Simply test we can find the jar files in our test folder
      List<String> jars = ch.findFiles( testFolder, "jar" );
      assertEquals( 4, jars.size() );

      // Look for all files and folders
      List<String> all = ch.findFiles( testFolder, null );
      assertEquals( 12, all.size() );
    } finally {
      testFolder.delete( new AllFileSelector() );
    }
  }

  @Test
  public void findFiles_vfs_hdfs() throws Exception {

    DistributedCacheUtilImpl ch = new DistributedCacheUtilImpl( TEST_CONFIG );

    URL url = new URL( "http://localhost:8020/path/to/file" );
    Configuration conf = mock( Configuration.class );
    FileSystem fs = mock( FileSystem.class );
    FileObject source = mock( FileObject.class );
    Path dest = mock( Path.class );
    FileObject hdfsDest = mock( FileObject.class );
    Path root = mock( Path.class );

    FileObject[] fileObjects = new FileObject[12];
    for ( int i = 0; i < fileObjects.length; i++ ) {
      URL fileUrl = new URL( "http://localhost:8020/path/to/file/" + i );
      FileObject fileObject = mock( FileObject.class );
      fileObjects[i] = fileObject;
      doReturn( fileUrl ).when( fileObject ).getURL();
    }

    doReturn( url ).when( source ).getURL();
    doReturn( conf ).when( fs ).getConf();
    doReturn( 0 ).when( conf ).getInt( any( String.class ), anyInt() );
    doReturn( true ).when( source ).exists();
    doReturn( fileObjects ).when( hdfsDest ).findFiles( any( FileSelector.class ) );
    doReturn( true ).when( fs ).delete( root, true );
    doReturn( fileObjects.length ).when( source ).delete( any( AllFileSelector.class ) );
    doNothing().when( fs ).copyFromLocalFile( any( Path.class ), any( Path.class ) );
    doNothing().when( fs ).setPermission( any( Path.class ), any( FsPermission.class ) );
    doReturn( true ).when( fs ).setReplication( any( Path.class ), anyShort() );

    try {
      try {
        ch.stageForCache( source, fs, dest, true );

        List<String> files = ch.findFiles( hdfsDest, null );
        assertEquals( 12, files.size() );
      } finally {
        fs.delete( root, true );
      }
    } finally {
      source.delete( new AllFileSelector() );
    }
  }

  @Test
  public void stageForCache_missing_source() throws Exception {
    DistributedCacheUtilImpl ch = new DistributedCacheUtilImpl( TEST_CONFIG );

    Configuration conf = new Configuration();
    FileSystem fs = DistributedCacheTestUtil.getLocalFileSystem( conf );

    Path dest = new Path( "bin/test/bogus-destination" );
    FileObject bogusSource = KettleVFS.getFileObject( "bogus" );
    try {
      ch.stageForCache( bogusSource, fs, dest, true );
      fail( "expected exception when source does not exist" );
    } catch ( KettleFileException ex ) {
      assertEquals( BaseMessages.getString( DistributedCacheUtilImpl.class, "DistributedCacheUtil.SourceDoesNotExist", bogusSource ), ex.getMessage().trim() );
    }
  }

  @Test
  public void stageForCache_destination_no_overwrite() throws Exception {
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
      try {
        ch.stageForCache( source, fs, dest, false );
      } catch ( KettleFileException ex ) {
        assertTrue( ex.getMessage(), ex.getMessage().contains( "Destination exists" ) );
      } finally {
        fs.delete( root, true );
      }
    } finally {
      source.delete( new AllFileSelector() );
    }
  }

  @Test
  public void addCachedFilesToClasspath() throws IOException {
    DistributedCacheUtilImpl ch = new DistributedCacheUtilImpl( TEST_CONFIG );
    Configuration conf = new Configuration();

    List<Path> files = Arrays.asList( new Path( "a" ), new Path( "b" ), new Path( "c" ) );

    ch.addCachedFilesToClasspath( files, conf );

    // this check is not needed for each and every shim
    if ( "true".equals( System.getProperty( "org.pentaho.hadoop.shims.check.symlink", "false" ) ) ) {
      assertEquals( "yes", conf.get( "mapred.create.symlink" ) );
    }

    for ( Path file : files ) {
      assertTrue( conf.get( "mapred.cache.files" ).contains( file.toString() ) );
      assertTrue( conf.get( "mapred.job.classpath.files" ).contains( file.toString() ) );
    }
  }

  @Test
  public void installKettleEnvironment_missing_arguments() throws Exception {
    DistributedCacheUtilImpl ch = new DistributedCacheUtilImpl( TEST_CONFIG );

    try {
      ch.installKettleEnvironment( null, (org.pentaho.hadoop.shim.api.fs.FileSystem) null, null, null, null );
      fail( "Expected exception on missing archive" );
    } catch ( NullPointerException ex ) {
      assertEquals( "pmrArchive is required", ex.getMessage() );
    }

    try {
      ch.installKettleEnvironment( KettleVFS.getFileObject( "." ), (org.pentaho.hadoop.shim.api.fs.FileSystem) null, null, null, null );
      fail( "Expected exception on missing archive" );
    } catch ( NullPointerException ex ) {
      assertEquals( "destination is required", ex.getMessage() );
    }

    try {
      ch.installKettleEnvironment( KettleVFS.getFileObject( "." ), (org.pentaho.hadoop.shim.api.fs.FileSystem) null, new PathProxy( "." ), null, null );
      fail( "Expected exception on missing archive" );
    } catch ( NullPointerException ex ) {
      assertEquals( "big data plugin required", ex.getMessage() );
    }
  }

  @Test( expected = IllegalArgumentException.class )
  public void stagePluginsForCache_no_folders() throws Exception {
    DistributedCacheUtilImpl ch = new DistributedCacheUtilImpl( TEST_CONFIG );
    ch.stagePluginsForCache( DistributedCacheTestUtil.getLocalFileSystem( new Configuration() ), new Path( "bin/test/plugins-installation-dir" ), null );
  }

  @Test( expected = KettleFileException.class )
  public void stagePluginsForCache_invalid_folder() throws Exception {
    DistributedCacheUtilImpl ch = new DistributedCacheUtilImpl( TEST_CONFIG );
    ch.stagePluginsForCache( DistributedCacheTestUtil.getLocalFileSystem( new Configuration() ), new Path( "bin/test/plugins-installation-dir" ), "bin/bogus-plugin-name" );
  }

  @Test
  public void findPluginFolder() throws Exception {
    DistributedCacheUtilImpl util = new DistributedCacheUtilImpl( TEST_CONFIG );

    // Fake out the "plugins" directory for the project's root directory
    String originalValue = System.getProperty( Const.PLUGIN_BASE_FOLDERS_PROP );
    System.setProperty( Const.PLUGIN_BASE_FOLDERS_PROP, KettleVFS.getFileObject( "." ).getURL().toURI().getPath() );

    assertNotNull( "Should have found plugin dir: bin/", util.findPluginFolder( "bin" ) );
    assertNotNull( "Should be able to find nested plugin dir: bin/test/", util.findPluginFolder( "bin/test" ) );

    assertNull( "Should not have found plugin dir: org/", util.findPluginFolder( "org" ) );
    System.setProperty( Const.PLUGIN_BASE_FOLDERS_PROP, originalValue );
  }

  @Test
  public void addFilesToClassPath() throws IOException {
    DistributedCacheUtilImpl util = new DistributedCacheUtilImpl( TEST_CONFIG );
    Path p1 = new Path( "/testing1" );
    Path p2 = new Path( "/testing2" );
    Configuration conf = new Configuration();
    util.addFileToClassPath( p1, conf );
    util.addFileToClassPath( p2, conf );
    assertEquals( "/testing1:/testing2", conf.get( "mapred.job.classpath.files" ) );
  }

  @Test
  public void addFilesToClassPath_custom_path_separator() throws IOException {
    DistributedCacheUtilImpl util = new DistributedCacheUtilImpl( TEST_CONFIG );
    Path p1 = new Path( "/testing1" );
    Path p2 = new Path( "/testing2" );
    Configuration conf = new Configuration();
    String originalValue = System.getProperty( "hadoop.cluster.path.separator", ":" );
    System.setProperty( "hadoop.cluster.path.separator", "J" );

    util.addFileToClassPath( p1, conf );
    util.addFileToClassPath( p2, conf );
    assertEquals( "/testing1J/testing2", conf.get( "mapred.job.classpath.files" ) );
    System.setProperty( "hadoop.cluster.path.separator", originalValue );
  }
}

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


package org.pentaho.hadoop.shim.common;

import org.apache.commons.vfs2.AllFileSelector;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.hadoop.shim.common.fs.PathProxy;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * Test the DistributedCacheUtil
 */
public class DistributedCacheUtilImplTest {

  private static String PLUGIN_BASE = null;

  @BeforeClass
  public static void setup() throws Exception {

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

  //@Test( expected = NullPointerException.class )
  public void instantiation() {
    new DistributedCacheUtilImpl();
  }

  @Test
  public void deleteDirectory() throws Exception {
    FileObject test = KettleVFS.getFileObject( "bin/test/deleteDirectoryTest" );
    test.createFolder();

    DistributedCacheUtilImpl ch = new DistributedCacheUtilImpl();
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
    DistributedCacheUtilImpl ch = new DistributedCacheUtilImpl();

    try {
      ch.extract( KettleVFS.getFileObject( "bogus" ), null );
      fail( "expected exception" );
    } catch ( IllegalArgumentException ex ) {
      assertTrue( ex.getMessage().startsWith( "archive does not exist" ) );
    }
  }

  @Test
  public void extract_destination_exists() throws Exception {
    DistributedCacheUtilImpl ch = new DistributedCacheUtilImpl();

    FileObject archive =
      KettleVFS.getFileObject( getClass().getResource( "/pentaho-mapreduce-sample.jar" ).toURI().getPath() );

    try {
      ch.extract( archive, KettleVFS.getFileObject( "." ) );
    } catch ( IllegalArgumentException ex ) {
      assertTrue( ex.getMessage(), "destination already exists".equals( ex.getMessage() ) );
    }
  }

  @Test
  public void extractToTemp() throws Exception {
    DistributedCacheUtilImpl ch = new DistributedCacheUtilImpl();

    FileObject archive =
      KettleVFS.getFileObject( getClass().getResource( "/pentaho-mapreduce-sample.jar" ).toURI().getPath() );
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
    DistributedCacheUtilImpl ch = new DistributedCacheUtilImpl();

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
    DistributedCacheUtilImpl ch = new DistributedCacheUtilImpl();

    try {
      ch.extractToTemp( null );
      fail( "Expected exception" );
    } catch ( NullPointerException ex ) {
      assertEquals( "archive is required", ex.getMessage() );
    }
  }

  @Test
  public void findFiles_vfs() throws Exception {
    DistributedCacheUtilImpl ch = new DistributedCacheUtilImpl();

    FileObject testFolder = DistributedCacheTestUtil.createTestFolderWithContent();

    try {
      // Simply test we can find the jar files in our test folder
      List<String> jars = ch.findFiles( testFolder, "jar" );
      assertEquals( 4, jars.size() );

      // Look for all files and folders
      List<String> all = ch.findFiles( testFolder, null );
      assertEquals( 15, all.size() );
    } finally {
      testFolder.delete( new AllFileSelector() );
    }
  }

  @Test
  public void findFiles_vfs_hdfs() throws Exception {

    DistributedCacheUtilImpl ch = new DistributedCacheUtilImpl();
    FileObject hdfsDest = mock( FileObject.class );


    FileObject[] fileObjects = new FileObject[ 12 ];
    for ( int i = 0; i < fileObjects.length; i++ ) {
      URL fileUrl = new URL( "http://localhost:8020/path/to/file/" + i );
      FileObject fileObject = mock( FileObject.class );
      fileObjects[ i ] = fileObject;
      doReturn( fileUrl ).when( fileObject ).getURL();
    }

    doReturn( fileObjects ).when( hdfsDest ).findFiles( any( FileSelector.class ) );

    try {
      List<String> files = ch.findFiles( hdfsDest, null );
      assertEquals( 12, files.size() );
    } catch ( Exception e ) {
      fail( e.getMessage() );
    }
  }

  @Test
  public void stageForCache_missing_source() throws Exception {
    DistributedCacheUtilImpl ch = new DistributedCacheUtilImpl();

    Configuration conf = new Configuration();
    FileSystem fs = DistributedCacheTestUtil.getLocalFileSystem( conf );

    Path dest = new Path( "bin/test/bogus-destination" );
    FileObject bogusSource = KettleVFS.getFileObject( "bogus" );
    try {
      ch.stageForCache( bogusSource, fs, dest, true );
      fail( "expected exception when source does not exist" );
    } catch ( KettleFileException ex ) {
      assertEquals( BaseMessages
          .getString( DistributedCacheUtilImpl.class, "DistributedCacheUtil.SourceDoesNotExist", bogusSource ),
        ex.getMessage().trim() );
    }
  }

  @Test
  public void stageForCache_destination_no_overwrite() throws Exception {
    DistributedCacheUtilImpl ch = new DistributedCacheUtilImpl();

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
  public void stageForCache_exclude_file() throws Exception {
    DistributedCacheUtilImpl ch = new DistributedCacheUtilImpl();

    Configuration conf = new Configuration();
    FileSystem fs = DistributedCacheTestUtil.getLocalFileSystem( conf );

    FileObject source = DistributedCacheTestUtil.createTestFolderWithContent( "sample-folder", 3 );
    try {
      Path root = new Path( "bin/test/stageForCache_exclude_file" );
      Path dest = new Path( root, "dest" );

      try {
        ch.stageForCache( source, fs, dest, "foo,jar2,bar,jar3", false, true );
        assertTrue( fs.exists( new Path( dest, "jar1.jar" ) ) );
        assertTrue( fs.exists( new Path( dest, "folder/file.txt" ) ) );
        assertTrue( fs.exists( new Path( dest, "pentaho-mapreduce-libraries.zip" ) ) );
        assertFalse( fs.exists( new Path( dest, "jar2.jar" ) ) );
        assertFalse( fs.exists( new Path( dest, "jar3.jar" ) ) );
      } finally {
        fs.delete( root, true );
      }
    } finally {
      source.delete( new AllFileSelector() );
    }
  }

  @Test
  public void addCachedFilesToClasspath() throws IOException {
    DistributedCacheUtilImpl ch = new DistributedCacheUtilImpl();
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
    DistributedCacheUtilImpl ch = new DistributedCacheUtilImpl();

    try {
      ch.installKettleEnvironment( null, (org.pentaho.hadoop.shim.api.internal.fs.FileSystem) null, null, null, null,
        "", "" );
      fail( "Expected exception on missing archive" );
    } catch ( NullPointerException ex ) {
      assertEquals( "pmrArchive is required", ex.getMessage() );
    }

    try {
      ch.installKettleEnvironment( KettleVFS.getFileObject( "." ),
        (org.pentaho.hadoop.shim.api.internal.fs.FileSystem) null, null, null, null, "", "" );
      fail( "Expected exception on missing archive" );
    } catch ( NullPointerException ex ) {
      assertEquals( "destination is required", ex.getMessage() );
    }

    try {
      ch.installKettleEnvironment( KettleVFS.getFileObject( "." ),
        (org.pentaho.hadoop.shim.api.internal.fs.FileSystem) null, new PathProxy( "." ), null, null, "", "" );
      fail( "Expected exception on missing archive" );
    } catch ( NullPointerException ex ) {
      assertEquals( "big data plugin required", ex.getMessage() );
    }
  }

  @Test( expected = IllegalArgumentException.class )
  public void stagePluginsForCache_no_folders() throws Exception {
    DistributedCacheUtilImpl ch = new DistributedCacheUtilImpl();
    ch.stagePluginsForCache( DistributedCacheTestUtil.getLocalFileSystem( new Configuration() ),
      new Path( "bin/test/plugins-installation-dir" ), null, "" );
  }

  @Test( expected = KettleFileException.class )
  public void stagePluginsForCache_invalid_folder() throws Exception {
    DistributedCacheUtilImpl ch = new DistributedCacheUtilImpl();
    ch.stagePluginsForCache( DistributedCacheTestUtil.getLocalFileSystem( new Configuration() ),
      new Path( "bin/test/plugins-installation-dir" ), "bin/bogus-plugin-name", "" );
  }

  @Test
  public void findPluginFolder() throws Exception {
    DistributedCacheUtilImpl util = new DistributedCacheUtilImpl();

    // Fake out the "plugins" directory for the project's root directory
    String originalValue = System.getProperty( Const.PLUGIN_BASE_FOLDERS_PROP );
    System.setProperty( Const.PLUGIN_BASE_FOLDERS_PROP, KettleVFS.getFileObject( "." ).getURL().toURI().getPath() );

    assertTrue( "Should have found plugin dir: bin/", util.findPluginFolder( "bin" ).length > 0 );
    assertTrue( "Should be able to find nested plugin dir: bin/test/", util.findPluginFolder( "bin/test" ).length > 0 );

    assertTrue( "Should not have found plugin dir: org/", util.findPluginFolder( "org" ).length == 0 );
    System.setProperty( Const.PLUGIN_BASE_FOLDERS_PROP, originalValue );
  }

  @Test
  public void addFilesToClassPath() throws IOException {
    DistributedCacheUtilImpl util = new DistributedCacheUtilImpl();
    Path p1 = new Path( "/testing1" );
    Path p2 = new Path( "/testing2" );
    Configuration conf = new Configuration();
    util.addFileToClassPath( p1, conf );
    util.addFileToClassPath( p2, conf );
    assertEquals( "/testing1,/testing2", conf.get( "mapred.job.classpath.files" ) );
  }

  @Test
  public void addFilesToClassPath_custom_path_separator() throws IOException {
    DistributedCacheUtilImpl util = new DistributedCacheUtilImpl();
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

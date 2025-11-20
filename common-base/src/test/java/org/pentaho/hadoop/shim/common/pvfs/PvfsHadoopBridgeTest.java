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


package org.pentaho.hadoop.shim.common.pvfs;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.connections.ConnectionDetails;
import org.pentaho.di.connections.ConnectionManager;
import org.pentaho.di.core.Const;
import org.pentaho.hadoop.shim.common.pvfs.conf.PvfsConf;

import java.io.File;
import java.io.IOException;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class PvfsHadoopBridgeTest {

  private final Path path = new Path( "pvfs://conn/input" );
  private static final String TMPFILE_CONTENTS = "testString";
  @Mock private PvfsConf.ConfFactory confFactory;
  @Mock private PvfsConf pvfsConf;
  @Mock private ConnectionManager connectionManager;
  @Mock private ConnectionDetails details;

  @Rule public TemporaryFolder folder = new TemporaryFolder();
  private File tempFile;
  private PvfsHadoopBridge bridge;

  @Before
  public void before() throws IOException {
    tempFile = folder.newFile( "pvfs.txt" );
    FileUtils.writeStringToFile( tempFile, TMPFILE_CONTENTS );
    when( confFactory.get( any( ConnectionDetails.class ) ) ).thenReturn( pvfsConf );
    when( pvfsConf.mapPath( any( Path.class ) ) ).thenReturn( new Path( tempFile.toURI() ) );
    when( pvfsConf.supportsConnection() ).thenReturn( true );
    when( pvfsConf.conf( any( Path.class ) ) ).thenReturn( new Configuration() );
    when( connectionManager.getConnectionDetails( nullable( String.class ) ) ).thenReturn( details );
    when( details.getType() ).thenReturn( "snw" );

    bridge = new PvfsHadoopBridge( singletonList( confFactory ), connectionManager );
  }

  @Test
  public void getScheme() {
    assertThat( bridge.getScheme(), equalTo( "pvfs" ) );
  }

  @Test
  public void makeQualified() {
    assertThat( bridge.makeQualified( path ),
      equalTo( path ) );
  }

  @Test
  public void open() throws IOException {
    byte[] content = new byte[ 10 ];
    try ( FSDataInputStream is = bridge.open( path, 5 ) ) {
      is.readFully( content );
    }
    assertThat( new String( content ), equalTo( TMPFILE_CONTENTS ) );
  }

  @Test
  public void openUnsupported() throws IOException {
    when( pvfsConf.supportsConnection() ).thenReturn( false );
    byte[] content = new byte[ 10 ];
    try ( FSDataInputStream is = bridge.open( path, 5 ) ) {
      is.readFully( content );
    } catch ( IllegalStateException e ) {
      assertEquals( "Unsupported VFS connection type:  snw", e.getMessage() );
    }
  }

  @Test
  public void checkPath() {
    Path unsupportedPath = new Path( "pvfs", "noSuchConnectionName", "/foo/bar.txt" );
    Path supportedPath = new Path( "pvfs", "definedConnection", "/foo/bar.txt" );
    when( pvfsConf.mapPath( unsupportedPath ) )
      .thenReturn( new Path( "badscheme", "badauthority", "/" ) );
    when( pvfsConf.mapPath( supportedPath ) )
      .thenReturn( new Path( "file", "", "/tmp" ) );

    try {
      bridge.checkPath( unsupportedPath );
      fail( "Expected exception" );
    } catch ( Exception e ) {
      assertTrue( e.getCause().getMessage().contains( "badscheme" ) );
    }
    bridge.checkPath( supportedPath );

  }

  @Test
  /**
   * Verifies proxying to the underlying fs implementation works correctly, using
   * the LocalFileSystem impl.
   */
  public void testIOOps() throws IOException {
    //Windows will fail this test due to winutils not being present. So don't run for windows
    assumeFalse( Const.isWindows() );

    assertTrue( bridge.getFileStatus( new Path( "pvfs", "", tempFile.getPath() ) ).isFile() );
    Path child = bridge.makeQualified( new Path( tempFile.getParent(), "child" ) );
    Path child2 = bridge.makeQualified( new Path( tempFile.getParent(), "child_renamed" ) );
    assertTrue( bridge.mkdirs( child ) );
    assertTrue( bridge.rename( child, child2 ) );
    assertTrue( new File( child2.toUri().getPath() ).exists() );
    assertTrue( bridge.getFileStatus( child2 ).isDirectory() );

    try ( FSDataOutputStream os = bridge.create( new Path( child2, "file" ) ) ) {
      os.writeChars( TMPFILE_CONTENTS );
    }
    assertTrue( new File( child2.toUri().getPath(), "file" ).exists() );

    if ( !Shell.WINDOWS ) {  //This causes a native link error in windows probably due to insufficient winutils
      FileStatus[] status = bridge.listStatus( child2 );
      assertThat( status.length, equalTo( 1 ) );
    }

    bridge.delete( child2, true );
    assertFalse( new File( child2.toUri().getPath(), "file" ).exists() );
  }

  @Test
  public void setGetWorkingDirectory() {
    Path pvfsPath = new Path( "pvfs", "", folder.getRoot().toURI().getPath() );
    Path newWd = new Path( folder.getRoot().toURI() );
    when( pvfsConf.mapPath( any( Path.class ) ) ).thenReturn( newWd );
    bridge.setWorkingDirectory( pvfsPath );
    assertThat( bridge.getWorkingDirectory(), equalTo( newWd ) );
  }

  @Test
  public void connectionNamesParsedCorrectly() {
    asList(
      "authority with spaces",
      "authorityMixedCase",
      "under_scores",
      "UPPERCASE",
      "Dashes-dashes",
      "!@!@$!@)(*)(*&*(&( {}|``~" ).forEach(
        connectionName -> {
          bridge.getConnectionDetails( new Path( "pvfs", connectionName, "/path/to/file" ) );
          verify( connectionManager ).getConnectionDetails( connectionName );
        } );

  }

  @Test
  public void generateAliasTest() {
    when( pvfsConf.generateAlias( any( String.class ) ) ).thenReturn( "aliasFileName" );
    assertEquals( "aliasFileName", bridge.generateAlias( "/anyPath" ) );
  }

}

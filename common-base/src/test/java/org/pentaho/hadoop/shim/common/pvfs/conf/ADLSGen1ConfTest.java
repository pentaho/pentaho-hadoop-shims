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

package org.pentaho.hadoop.shim.common.pvfs.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.connections.ConnectionDetails;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class ADLSGen1ConfTest {

  private ADLSGen1Conf adlsGen1Conf;
  private ADLSGen1Conf badADLSGen1Conf;
  private S3Conf s3Conf;
  private Path path;
  @Mock
  private ConnectionDetails adlsGen1Conn;
  @Mock private ConnectionDetails otherADLS1Conn;
  @Mock private ConnectionDetails hcpConn;
  @Mock private ConnectionDetails s3Conn;
  private Map<String, String> adlsGen1Props = new HashMap<>();
  private Map<String, String> s3Props = new HashMap<>();

  @Before
  public void before() {
    adlsGen1Props.put( "accountFQDN", "mockAccountName.azuredatalakestore.net" );
    adlsGen1Props.put( "clientId", "mOckSharedKey==" );
    adlsGen1Props.put( "clientSecret", "mockAccountName" );
    adlsGen1Props.put( "authTokenEndpoint", "https://login.microsoftonline.com/123/oauth2/token" );
    when( adlsGen1Conn.getProperties() ).thenReturn( adlsGen1Props );
    when( adlsGen1Conn.getType() ).thenReturn( "adl" );
    adlsGen1Conf = new ADLSGen1Conf( adlsGen1Conn );


    path = new Path( "pvfs://gen1Conn/mockContainer/mockFile.txt" );

    s3Props.put( "accessKey", "ACCESSKEY" );
    s3Props.put( "secretKey", "SECRETKEY" );
    when( s3Conn.getProperties() ).thenReturn( s3Props );
    s3Conf = new S3Conf( s3Conn );

    when( hcpConn.getType() ).thenReturn( "hcp" );
    badADLSGen1Conf = new ADLSGen1Conf( hcpConn );
  }

  @Test
  public void testSupportedSchemes() {
    assertTrue( adlsGen1Conf.supportsConnection() );
    assertFalse( badADLSGen1Conf.supportsConnection() );
  }

  @Test public void mapPath() {
    Path result = adlsGen1Conf.mapPath( path );
    assertEquals( "adl://mockAccountName.azuredatalakestore.net/mockContainer/mockFile.txt", result.toString() );
  }

  @Test public void mapPathWithSpaces() {
    Path pathWithSpaces = new Path( "pvfs://gen1 Conn/mockContainer/mockFile.txt" );
    Path result = adlsGen1Conf.mapPath( pathWithSpaces );
    assertEquals( "adl://mockAccountName.azuredatalakestore.net/mockContainer/mockFile.txt", result.toString() );
  }

  @Test public void testConf() {
    Configuration conf = adlsGen1Conf.conf( path );
    assertEquals( "org.apache.hadoop.fs.adl.AdlFileSystem", conf.get( "fs.adl.impl" ) );
    assertEquals( "org.apache.hadoop.fs.adl.Adl", conf.get( "fs.AbstractFileSystem.adl.impl" ) );
  }

  @Test public void testEquals() {
    assertNotEquals( null, adlsGen1Conf );
    assertEquals( adlsGen1Conf, adlsGen1Conf );
    assertNotEquals( adlsGen1Conf, s3Conf );
    when( otherADLS1Conn.getProperties() ).thenReturn( new HashMap<>( adlsGen1Props ) );

    ADLSGen1Conf otherGen1Conf = new ADLSGen1Conf( otherADLS1Conn );

    assertEquals( otherGen1Conf, adlsGen1Conf );
    // change auth credentials path
    otherADLS1Conn.getProperties().put( "sharedKey", "othermOckSharedKey==" );
    assertNotEquals( otherGen1Conf, adlsGen1Conf );

    assertNotEquals( adlsGen1Conf.hashCode(), s3Conf.hashCode() );
    assertNotEquals( adlsGen1Conf.hashCode(), otherGen1Conf.hashCode() );
  }
}

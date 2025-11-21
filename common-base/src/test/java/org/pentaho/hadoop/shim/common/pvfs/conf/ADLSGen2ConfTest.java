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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class ADLSGen2ConfTest {

  private ADLSGen2Conf adlsGen2Conf;
  private ADLSGen2Conf badADLSGen2Conf;
  private S3Conf s3Conf;
  private Path path;
  @Mock
  private ConnectionDetails adlsGen2Conn;
  @Mock private ConnectionDetails otherADLS2Conn;
  @Mock private ConnectionDetails hcpConn;
  @Mock private ConnectionDetails s3Conn;
  private Map<String, String> adlsGen2Props = new HashMap<>();
  private Map<String, String> s3Props = new HashMap<>();

  @Before
  public void before() {
    adlsGen2Props.put( "accountName", "mockAccountName" );
    adlsGen2Props.put( "sharedKey", "mOckSharedKey==" );
    //adlsGen2Props.put( "accountName", "mockAccountName" );
    when( adlsGen2Conn.getProperties() ).thenReturn( adlsGen2Props );
    when( adlsGen2Conn.getType() ).thenReturn( "abfss" );
    adlsGen2Conf = new ADLSGen2Conf( adlsGen2Conn );


    path = new Path( "pvfs://sharedConn/mockContainer/mockFile.txt" );

    s3Props.put( "accessKey", "ACCESSKEY" );
    s3Props.put( "secretKey", "SECRETKEY" );
    when( s3Conn.getProperties() ).thenReturn(s3Props);
    s3Conf = new S3Conf( s3Conn );

    when( hcpConn.getType() ).thenReturn( "hcp" );
    badADLSGen2Conf = new ADLSGen2Conf( hcpConn );
  }

  @Test
  public void testSupportedSchemes() {
    assertTrue( adlsGen2Conf.supportsConnection() );
    assertFalse( badADLSGen2Conf.supportsConnection() );
  }

  @Test public void mapPath() {
    Path result = adlsGen2Conf.mapPath( path );
    assertThat( result.toString(), equalTo( "abfss://mockContainer@mockAccountName.dfs.core.windows.net/mockFile.txt" ) );
  }

  @Test public void mapPathWithSpaces() {
    Path pathWithSpaces = new Path( "pvfs://shared Conn/mockContainer/mockFile.txt" );
    Path result = adlsGen2Conf.mapPath( pathWithSpaces );
    assertThat( result.toString(), equalTo( "abfss://mockContainer@mockAccountName.dfs.core.windows.net/mockFile.txt" ) );
  }

  @Test public void testConf() {
    Configuration conf = adlsGen2Conf.conf( path );
    assertThat( conf.get( "fs.abfss.impl" ),
        equalTo( "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem" ) );
    assertThat( conf.get( "fs.azure.abfss.account.name" ),
        equalTo( "mockAccountName.dfs.core.windows.net" ) );
  }

  @Test public void testEquals() {
    assertNotEquals( null, adlsGen2Conf);
    assertEquals(adlsGen2Conf, adlsGen2Conf);
    assertNotEquals(adlsGen2Conf, s3Conf );
    when( otherADLS2Conn.getProperties() ).thenReturn( new HashMap<>(adlsGen2Props) );

    ADLSGen2Conf otherGcsConf = new ADLSGen2Conf( otherADLS2Conn );

    assertEquals( otherGcsConf, adlsGen2Conf);
    // change auth credentials path
    otherADLS2Conn.getProperties().put( "sharedKey", "othermOckSharedKey==" );
    assertNotEquals( otherGcsConf, adlsGen2Conf);

    assertNotEquals( adlsGen2Conf.hashCode(), s3Conf.hashCode() );
    assertNotEquals( adlsGen2Conf.hashCode(), otherGcsConf.hashCode() );
  }
}

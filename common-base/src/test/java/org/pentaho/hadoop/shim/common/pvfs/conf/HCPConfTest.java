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

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class HCPConfTest {

  private HCPConf hcpConf;
  private HCPConf badHcpConf;
  private Path path;
  @Mock private ConnectionDetails hcpConn;
  @Mock private ConnectionDetails s3Conn;

  @SuppressWarnings( "squid:S2068" )
  @Before public void before() {
    Map<String, String> props = new HashMap<>();
    props.put( "namespace", "nstest" );
    props.put( "host", "hcp.server.net" );
    props.put( "username", "suzy" );
    props.put( "password", "p@ssw3rd" );
    props.put( "tenant", "r2d2" );
    props.put( "proxyHost", "proxyserver" );
    props.put( "proxyPort", "8888" );

    when( hcpConn.getType() ).thenReturn( "hcp" );
    when( hcpConn.getProperties() ).thenReturn( props );
    when( s3Conn.getType() ).thenReturn( "s3" );
    hcpConf = new HCPConf( hcpConn );
    badHcpConf = new HCPConf( s3Conn );
    path = new Path( "pvfs://namedConn/somedir/somechild" );
  }

  @Test public void testSupportedSchemes() {
    assertTrue( hcpConf.supportsConnection() );
    assertFalse( badHcpConf.supportsConnection() );
  }

  @Test public void mapPath() {
    Path result = hcpConf.mapPath( path );
    assertThat( result.toString(), equalTo( "s3a://nstest/somedir/somechild" ) );
    assertThat( hcpConf.mapPath( path, new Path( "s3a://nstest/dir/file" ) ).toString(),
      equalTo( "pvfs://namedConn/dir/file" ) );
  }

  @Test public void mapPathWithSpaces() {
    Path pathWithSpaces = new Path( "pvfs://nam ed Conn/somedir/somechild" );
    Path result = hcpConf.mapPath( pathWithSpaces );
    assertThat( result.toString(), equalTo( "s3a://nstest/somedir/somechild" ) );
    assertThat( hcpConf.mapPath( pathWithSpaces, new Path( "s3a://nstest/dir/file" ) ).toString(),
      equalTo( "pvfs://nam ed Conn/dir/file" ) );
  }

  @Test public void testConf() {
    Configuration conf = hcpConf.conf( path );
    assertThat( conf.get( "fs.s3a.endpoint" ), equalTo( "r2d2.hcp.server.net" ) );
    assertThat( conf.get( "fs.s3a.access.key" ), equalTo( Base64.getEncoder().encodeToString( "suzy".getBytes() ) ) );
    assertThat( conf.get( "fs.s3a.proxy.host" ), equalTo( "proxyserver" ) );
  }
}

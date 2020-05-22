/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2019-2020 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.hadoop.shim.pvfs.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.connections.ConnectionDetails;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class GcsConfTest {

  private GcsConf gcsConf;
  private GcsConf badGcsConf;
  private S3Conf s3Conf;
  private Path path;
  @Mock private ConnectionDetails gcsConn;
  @Mock private ConnectionDetails otherGcsConn;
  @Mock private ConnectionDetails hcpConn;
  @Mock private ConnectionDetails s3Conn;
  private Map<String, String> gcsProps = new HashMap<>();
  private Map<String, String> s3Props = new HashMap<>();

  @Before public void before() {
    gcsProps.put( "keyPath", "/home/ubuntu/gcs/credentials.json" );
    when( gcsConn.getProperties() ).thenReturn(gcsProps);
    when( gcsConn.getType() ).thenReturn( "gs" );
    gcsConf = new GcsConf(gcsConn);
    badGcsConf = new GcsConf( gcsConn );
    path = new Path( "pvfs://namedConn/bucket/somedir/somechild" );

    s3Props.put( "accessKey", "ACCESSKEY" );
    s3Props.put( "secretKey", "SECRETKEY" );
    when( s3Conn.getProperties() ).thenReturn(s3Props);
    when( s3Conn.getType() ).thenReturn( "s3" );
    s3Conf = new S3Conf( s3Conn );

    when( hcpConn.getType() ).thenReturn( "hcp" );
    badGcsConf = new GcsConf( hcpConn );
  }

  @Test public void testSupportedSchemes() {
    assertTrue( gcsConf.supportsConnection() );
    assertFalse( badGcsConf.supportsConnection() );
  }

  @Test public void mapPath() {
    Path result = gcsConf.mapPath( path );
    assertThat( result.toString(), equalTo( "gs://bucket/somedir/somechild" ) );

    assertThat( gcsConf.mapPath( path, new Path( "gs://bucket/dir/file" ) ).toString(),
      equalTo( "pvfs://namedConn/bucket/dir/file" ) );
  }

  @Test public void mapPathWithSpaces() {
    Path pathWithSpaces = new Path( "pvfs://nam ed Conn/bucket/somedir/somechild" );
    Path result = gcsConf.mapPath( pathWithSpaces );
    assertThat( result.toString(), equalTo( "gs://bucket/somedir/somechild" ) );

    assertThat( gcsConf.mapPath( pathWithSpaces, new Path( "gs://bucket/dir/file" ) ).toString(),
      equalTo( "pvfs://nam ed Conn/bucket/dir/file" ) );
  }

  @Test public void testConf() {
    Configuration conf = gcsConf.conf( path );
    assertThat( conf.get( "fs.gs.impl" ),
      equalTo( "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem" ) );
    assertThat( conf.get( "google.cloud.auth.service.account.json.keyfile" ),
      equalTo( "/home/ubuntu/gcs/credentials.json" ) );
  }

  @Test public void testEquals() {
    assertNotEquals( null, gcsConf);
    assertEquals(gcsConf, gcsConf);
    assertNotEquals(gcsConf, s3Conf );
    when( otherGcsConn.getProperties() ).thenReturn( new HashMap<>(gcsProps) );
    when( otherGcsConn.getType() ).thenReturn( "gs" );

    GcsConf otherGcsConf = new GcsConf( otherGcsConn );

    assertEquals( otherGcsConf, gcsConf);
    // change auth credentials path
    otherGcsConn.getProperties().put( "keyPath", "/home/ubuntu/gcs/credentials2.json" );
    assertNotEquals( otherGcsConf, gcsConf);

    assertNotEquals( gcsConf.hashCode(), s3Conf.hashCode() );
    assertNotEquals( gcsConf.hashCode(), otherGcsConf.hashCode() );
  }

}

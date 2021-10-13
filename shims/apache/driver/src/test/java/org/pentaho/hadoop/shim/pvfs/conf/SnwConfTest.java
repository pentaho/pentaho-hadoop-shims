/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2020-2021 by Hitachi Vantara : http://www.pentaho.com
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class SnwConfTest {
  private SnwConf snwConf;
  private SnwConf badSnwConf;
  private Path path;
  @Mock private ConnectionDetails hcpConn;
  @Mock private ConnectionDetails snwConn;
  @Mock private ConnectionDetails otherSnwConn;
  private Map<String, String> props = new HashMap<>();

  @Before public void before() {
    when( hcpConn.getType() ).thenReturn( "hcp" );
    when( snwConn.getProperties() ).thenReturn( props );
    when( snwConn.getType() ).thenReturn( "snw" );
    snwConf = new SnwConf( snwConn );
    badSnwConf = new SnwConf( hcpConn );
    path = new Path( "pvfs://pvfsName/@stagedir/somedir/somechild" );
  }

  @Test public void testSupportedSchemes() {
    assertTrue( snwConf.supportsConnection() );
    assertFalse( badSnwConf.supportsConnection() );
  }

  @Test( expected = UnsupportedOperationException.class )
  public void testMapPath1() {
    snwConf.mapPath( path );
  }

  @Test( expected = UnsupportedOperationException.class )
  public void testmapPath2() {
    snwConf.mapPath( path, path );
  }

  @Test
  public void testGenerateAlias() {
    String aliasName = snwConf.generateAlias( path.toString() );
    assertNotNull( aliasName );
    assertNotEquals( path.toString(), aliasName );

    aliasName = snwConf.generateAlias( "pvfs://pvfsName/@%stagedir/somefile" );
    assertNotNull( aliasName );

    aliasName = snwConf.generateAlias( "pvfs://pvfsName/@~stagedir/somefile" );
    assertNotNull( aliasName );
  }

  @Test( expected = IllegalStateException.class )
  public void testGenerateAliasWithBadPath() {
    String aliasName = snwConf.generateAlias( "pvfs://pvfsName/stagedir/somefile" );
  }

  @Test public void testEquals() {
    assertNotEquals( null, snwConf );
    assertEquals( snwConf, snwConf );
    assertNotEquals( snwConf, hcpConn );
    when( otherSnwConn.getProperties() ).thenReturn( new HashMap<>( props ) );

    SnwConf otherSnwConf = new SnwConf( otherSnwConn );

    assertEquals( otherSnwConf, snwConf );
  }
}

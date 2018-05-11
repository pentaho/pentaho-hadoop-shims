/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.hadoop.shim.spi;

import org.junit.Test;
import org.pentaho.hadoop.shim.spi.HBaseConnection;

import java.net.URL;

import static org.junit.Assert.*;


/**
 * User: Dzmitry Stsiapanau Date: 10/16/2015 Time: 08:38
 */
public class HBaseConnectionTest {

  @Test
  public void testStringToURL() throws Exception {
    assertEquals( null, HBaseConnection.stringToURL( null ) );
    assertEquals( new URL( "http:///" ), HBaseConnection.stringToURL( "http:///" ) );
    assertEquals( new URL( "file:///" ), HBaseConnection.stringToURL( "file:///" ) );
    assertEquals( new URL( "file:///" ), HBaseConnection.stringToURL( "/" ) );
  }

  @Test
  public void testIsEmpty() throws Exception {
    assertTrue( HBaseConnection.isEmpty( null ) );
    assertTrue( HBaseConnection.isEmpty( "" ) );
    assertFalse( HBaseConnection.isEmpty( " " ) );
  }
}

/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
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

/*******************************************************************************
 * Pentaho Big Data
 * <p/>
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 * <p/>
 * ******************************************************************************
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 ******************************************************************************/

package org.pentaho.hbase.shim.common;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class CommonHBaseBytesUtilTest {

  private CommonHBaseBytesUtil util;

  @Before
  public void setUp() {
    util = new CommonHBaseBytesUtil();
  }

  @Test
  public void testGetSizeOfFloat() throws Exception {
    assertEquals( Bytes.SIZEOF_FLOAT, util.getSizeOfFloat() );
  }

  @Test
  public void testGetSizeOfDouble() throws Exception {
    assertEquals( Bytes.SIZEOF_DOUBLE, util.getSizeOfDouble() );
  }

  @Test
  public void testGetSizeOfInt() throws Exception {
    assertEquals( Bytes.SIZEOF_INT, util.getSizeOfInt() );
  }

  @Test
  public void testGetSizeOfLong() throws Exception {
    assertEquals( Bytes.SIZEOF_LONG, util.getSizeOfLong() );
  }

  @Test
  public void testGetSizeOfShort() throws Exception {
    assertEquals( Bytes.SIZEOF_SHORT, util.getSizeOfShort() );
  }

  @Test
  public void testGetSizeOfByte() throws Exception {
    assertEquals( Bytes.SIZEOF_BYTE, util.getSizeOfByte() );
  }

  @Test
  public void testToBytes_String() throws Exception {
    assertArrayEquals( Bytes.toBytes( "testToBytes_String" ), util.toBytes( "testToBytes_String" ) );
  }

  @Test
  public void testToBytes_boolean() throws Exception {
    assertArrayEquals( Bytes.toBytes( false ), util.toBytes( false ) );
  }

  @Test
  public void testToBytes_int() throws Exception {
    assertArrayEquals( Bytes.toBytes( 777 ), util.toBytes( 777 ) );
  }

  @Test
  public void testToBytes_long() throws Exception {
    assertArrayEquals( Bytes.toBytes( 123L ), util.toBytes( 123L ) );
  }

  @Test
  public void testToBytes_float() throws Exception {
    assertArrayEquals( Bytes.toBytes( 123.45F ), util.toBytes( 123.45F ) );
  }

  @Test
  public void testToBytes_double() throws Exception {
    assertArrayEquals( Bytes.toBytes( 567.89D ), util.toBytes( 567.89D ) );
  }

  @Test
  public void testToBytesBinary() throws Exception {
    assertArrayEquals( Bytes.toBytesBinary( "testToBytesBinary" ), util.toBytesBinary( "testToBytesBinary" ) );
  }

  @Test
  public void testToString() throws Exception {
    assertEquals( Bytes.toString( "testToString".getBytes() ), util.toString( "testToString".getBytes() ) );
  }

  @Test
  public void testToLong() throws Exception {
    byte[] bytes = Bytes.toBytes( 123L );
    assertEquals( Bytes.toLong( bytes ), util.toLong( bytes ) );
  }

  @Test
  public void testToInt() throws Exception {
    byte[] bytes = Bytes.toBytes( 1234 );
    assertEquals( Bytes.toInt( bytes ), util.toInt( bytes ) );
  }

  @Test
  public void testToFloat() throws Exception {
    byte[] bytes = Bytes.toBytes( 123.45F );
    assertEquals( Bytes.toFloat( bytes ), util.toFloat( bytes ), 0.0 );
  }

  @Test
  public void testToDouble() throws Exception {
    byte[] bytes = Bytes.toBytes( 123.45D );
    assertEquals( Bytes.toDouble( bytes ), util.toDouble( bytes ), 0.0D );
  }

  @Test
  public void testToShort() throws Exception {
    byte[] bytes = Bytes.toBytes( 1 );
    assertEquals( Bytes.toShort( bytes ), util.toShort( bytes ) );
  }

}

/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

package org.pentaho.hbase.shim.api;

import org.junit.After;
import org.junit.Test;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.row.value.ValueMetaBigNumber;
import org.pentaho.di.core.row.value.ValueMetaBinary;
import org.pentaho.di.core.row.value.ValueMetaBoolean;
import org.pentaho.di.core.row.value.ValueMetaDate;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaNumber;
import org.pentaho.di.core.row.value.ValueMetaSerializable;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.hbase.shim.spi.MockHBaseBytesUtilShim;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 * User: Dzmitry Stsiapanau
 * Date: 10/16/2015
 * Time: 08:38
 */

public class HBaseValueMetaTest extends HBaseValueMeta {


  private static final String DEF_NAME = "col_family,col_name,alias";
  private static final int DEF_TYPE = 0;
  private static final int DEF_LENGTH = 0;
  private static final int DEF_PRECISION = 0;
  public static final MockHBaseBytesUtilShim BYTES_UTIL = new MockHBaseBytesUtilShim();

  public HBaseValueMetaTest() throws IllegalArgumentException {
    super( DEF_NAME, DEF_TYPE, DEF_LENGTH, DEF_PRECISION );
  }

  private HBaseValueMeta getHBaseValueMeta() {
    HBaseValueMeta hbMeta = this;
    return hbMeta;
  }

  @After
  public void tearDown() throws Exception {
    reset();

  }

  private void reset() {
  }

  @Test
  public void testSetTableName() throws Exception {
    HBaseValueMeta hbMeta = getHBaseValueMeta();
    hbMeta.setTableName( "test" );
    assertEquals( "test", hbMeta.m_tableName );
  }

  @Test
  public void testGetTableName() throws Exception {
    HBaseValueMeta hbMeta = getHBaseValueMeta();
    hbMeta.m_tableName = "test";
    assertEquals( "test", hbMeta.getTableName() );
  }

  @Test
  public void testSetMappingName() throws Exception {
    HBaseValueMeta hbMeta = getHBaseValueMeta();
    hbMeta.setMappingName( "test" );
    assertEquals( "test", hbMeta.m_mappingName );
  }

  @Test
  public void testGetMappingName() throws Exception {
    HBaseValueMeta hbMeta = getHBaseValueMeta();
    hbMeta.m_mappingName = "test";
    assertEquals( "test", hbMeta.getMappingName() );
  }

  @Test
  public void testSetColumnFamily() throws Exception {
    HBaseValueMeta hbMeta = getHBaseValueMeta();
    hbMeta.setColumnFamily( "test" );
    assertEquals( "test", hbMeta.m_columnFamily );
  }

  @Test
  public void testGetColumnFamily() throws Exception {
    HBaseValueMeta hbMeta = getHBaseValueMeta();
    hbMeta.m_columnFamily = "test";
    assertEquals( "test", hbMeta.getColumnFamily() );
  }

  @Test
  public void testSetAlias() throws Exception {
    HBaseValueMeta hbMeta = getHBaseValueMeta();
    hbMeta.setAlias( "test" );
    assertEquals( "test", this.name );
  }

  @Test
  public void testGetAlias() throws Exception {
    HBaseValueMeta hbMeta = getHBaseValueMeta();
    this.name = "test";
    assertEquals( "test", hbMeta.getAlias() );
  }

  @Test
  public void testSetColumnName() throws Exception {
    HBaseValueMeta hbMeta = getHBaseValueMeta();
    hbMeta.setColumnName( "test" );
    assertEquals( "test", hbMeta.m_columnName );
  }

  @Test
  public void testGetColumnName() throws Exception {
    HBaseValueMeta hbMeta = getHBaseValueMeta();
    hbMeta.m_columnName = "test";
    assertEquals( "test", hbMeta.getColumnName() );
  }

  @Test
  public void testSetHBaseTypeFromString() throws Exception {
    KettleEnvironment.init();
    HBaseValueMeta hbMeta = getHBaseValueMeta();
    hbMeta.setHBaseTypeFromString( "Integer" );
    assertEquals( 5, this.type );
    assertEquals( false, hbMeta.m_isLongOrDouble );
    hbMeta.setHBaseTypeFromString( "Long" );
    assertEquals( 5, this.type );
    assertEquals( true, hbMeta.m_isLongOrDouble );
    hbMeta.setHBaseTypeFromString( "Float" );
    assertEquals( 1, this.type );
    assertEquals( false, hbMeta.m_isLongOrDouble );
    hbMeta.setHBaseTypeFromString( "Double" );
    assertEquals( 1, this.type );
    assertEquals( true, hbMeta.m_isLongOrDouble );
  }

  @Test
  public void testGetHBaseTypeDesc() throws Exception {
    KettleEnvironment.init();
    HBaseValueMeta hbMeta = getHBaseValueMeta();
    this.type = 5;
    hbMeta.m_isLongOrDouble = false;
    assertEquals( "Integer", hbMeta.getHBaseTypeDesc() );
    hbMeta.m_isLongOrDouble = true;
    assertEquals( "Long", hbMeta.getHBaseTypeDesc() );
    this.type = 1;
    hbMeta.m_isLongOrDouble = false;
    assertEquals( "Float", hbMeta.getHBaseTypeDesc() );
    hbMeta.m_isLongOrDouble = true;
    assertEquals( "Double", hbMeta.getHBaseTypeDesc() );
  }

  @Test
  public void testSetIsLongOrDouble() throws Exception {
    HBaseValueMeta hbMeta = getHBaseValueMeta();
    hbMeta.setIsLongOrDouble( true );
    assertTrue( hbMeta.m_isLongOrDouble );
  }

  @Test
  public void testGetIsLongOrDouble() throws Exception {
    HBaseValueMeta hbMeta = getHBaseValueMeta();
    hbMeta.m_isLongOrDouble = true;
    assertTrue( hbMeta.getIsLongOrDouble() );
  }

  @Test
  public void testSetKey() throws Exception {
    HBaseValueMeta hbMeta = getHBaseValueMeta();
    hbMeta.setKey( true );
    assertTrue( "test", hbMeta.m_isKey );
  }

  @Test
  public void testIsKey() throws Exception {
    HBaseValueMeta hbMeta = getHBaseValueMeta();
    hbMeta.m_isKey = true;
    assertTrue( "test", hbMeta.isKey() );
  }

  @Test
  public void testEncodeKeyValueWithMeta() throws Exception {
    HBaseValueMeta hbMeta = getHBaseValueMeta();
    Object keyValue = 1;
    assertArrayEquals( new byte[] { 1 },
      HBaseValueMeta.encodeKeyValue( keyValue, new ValueMetaString(), Mapping.KeyType.STRING, BYTES_UTIL ) );
    keyValue = new Date();
    assertArrayEquals( new byte[] { 3 },
      HBaseValueMeta.encodeKeyValue( keyValue, new ValueMetaDate(), Mapping.KeyType.DATE, BYTES_UTIL ) );
    keyValue = new Date();
    assertArrayEquals( new byte[] { 3 },
      HBaseValueMeta.encodeKeyValue( keyValue, new ValueMetaDate(), Mapping.KeyType.UNSIGNED_DATE, BYTES_UTIL ) );
    keyValue = (long) 0;
    assertArrayEquals( new byte[] { 2 },
      HBaseValueMeta.encodeKeyValue( keyValue, new ValueMetaInteger(), Mapping.KeyType.INTEGER, BYTES_UTIL ) );
    keyValue = (long) 0;
    assertArrayEquals( new byte[] { 2 },
      HBaseValueMeta.encodeKeyValue( keyValue, new ValueMetaInteger(), Mapping.KeyType.UNSIGNED_INTEGER, BYTES_UTIL ) );
    keyValue = (double) 0;
    assertArrayEquals( new byte[] { 3 },
      HBaseValueMeta.encodeKeyValue( keyValue, new ValueMetaNumber(), Mapping.KeyType.LONG, BYTES_UTIL ) );
    keyValue = (double) 0;
    assertArrayEquals( new byte[] { 3 },
      HBaseValueMeta.encodeKeyValue( keyValue, new ValueMetaNumber(), Mapping.KeyType.UNSIGNED_LONG, BYTES_UTIL ) );
    keyValue = new byte[] { 3 };
    assertArrayEquals( (byte[]) keyValue,
      HBaseValueMeta.encodeKeyValue( keyValue, new ValueMetaBinary(), Mapping.KeyType.BINARY, BYTES_UTIL ) );
  }

  @Test
  public void testEncodeKeyValue() throws Exception {
    HBaseValueMeta hbMeta = getHBaseValueMeta();
    Object keyValue = "1";
    assertArrayEquals( new byte[] { 1 },
      HBaseValueMeta.encodeKeyValue( keyValue, Mapping.KeyType.STRING, BYTES_UTIL ) );
    keyValue = new Date();
    assertArrayEquals( new byte[] { 3 },
      HBaseValueMeta.encodeKeyValue( keyValue, Mapping.KeyType.DATE, BYTES_UTIL ) );
    keyValue = new Date();
    assertArrayEquals( new byte[] { 3 },
      HBaseValueMeta.encodeKeyValue( keyValue, Mapping.KeyType.UNSIGNED_DATE, BYTES_UTIL ) );
    keyValue = 0;
    assertArrayEquals( new byte[] { 2 },
      HBaseValueMeta.encodeKeyValue( keyValue, Mapping.KeyType.INTEGER, BYTES_UTIL ) );
    keyValue = 0;
    assertArrayEquals( new byte[] { 2 },
      HBaseValueMeta.encodeKeyValue( keyValue, Mapping.KeyType.UNSIGNED_INTEGER, BYTES_UTIL ) );
    keyValue = (long) 0;
    assertArrayEquals( new byte[] { 3 },
      HBaseValueMeta.encodeKeyValue( keyValue, Mapping.KeyType.LONG, BYTES_UTIL ) );
    keyValue = (long) 0;
    assertArrayEquals( new byte[] { 3 },
      HBaseValueMeta.encodeKeyValue( keyValue, Mapping.KeyType.UNSIGNED_LONG, BYTES_UTIL ) );
    keyValue = new byte[] { 3 };
    assertArrayEquals( (byte[]) keyValue,
      HBaseValueMeta.encodeKeyValue( keyValue, Mapping.KeyType.BINARY, BYTES_UTIL ) );
  }

  @Test
  public void testEncodeKeyValueWithString() throws Exception {
    HBaseValueMeta hbMeta = getHBaseValueMeta();
    String keyValue = "1";
    assertArrayEquals( new byte[] { 1 },
      HBaseValueMeta.encodeKeyValue( keyValue, Mapping.KeyType.STRING, BYTES_UTIL ) );
    keyValue = new Date().toString();
    try {
      HBaseValueMeta.encodeKeyValue( keyValue, Mapping.KeyType.DATE, BYTES_UTIL );
      fail( "For date should be exception" );
    } catch ( KettleException e ) {
      //expected
    }
    keyValue = new Date().toString();
    try {
      HBaseValueMeta.encodeKeyValue( keyValue, Mapping.KeyType.UNSIGNED_DATE, BYTES_UTIL );
      fail( "For date should be exception" );
    } catch ( KettleException e ) {
      //expected
    }
    keyValue = Integer.toString( 0 );
    assertArrayEquals( new byte[] { 2 },
      HBaseValueMeta.encodeKeyValue( keyValue, Mapping.KeyType.INTEGER, BYTES_UTIL ) );
    keyValue = Integer.toString( 0 );
    assertArrayEquals( new byte[] { 2 },
      HBaseValueMeta.encodeKeyValue( keyValue, Mapping.KeyType.UNSIGNED_INTEGER, BYTES_UTIL ) );
    keyValue = Long.toString( 0 );
    assertArrayEquals( new byte[] { 3 },
      HBaseValueMeta.encodeKeyValue( keyValue, Mapping.KeyType.LONG, BYTES_UTIL ) );
    keyValue = Long.toString( 0 );
    assertArrayEquals( new byte[] { 3 },
      HBaseValueMeta.encodeKeyValue( keyValue, Mapping.KeyType.UNSIGNED_LONG, BYTES_UTIL ) );
    keyValue = new Object().toString();
    assertArrayEquals( new byte[] { 6 },
      HBaseValueMeta.encodeKeyValue( keyValue, Mapping.KeyType.BINARY, BYTES_UTIL ) );
  }

  @Test
  public void testDecodeKeyValue() throws Exception {
    HBaseValueMeta hbMeta = getHBaseValueMeta();
    Mapping mapping = new Mapping();
    mapping.setKeyType( Mapping.KeyType.STRING );
    Object decodedValue = "1";
    assertEquals( decodedValue, HBaseValueMeta.decodeKeyValue( new byte[] { 1 }, mapping, BYTES_UTIL ) );
    mapping.setKeyType( Mapping.KeyType.BINARY );
    decodedValue = new byte[] { 1 };
    assertEquals( decodedValue, HBaseValueMeta.decodeKeyValue( (byte[]) decodedValue, mapping, BYTES_UTIL ) );
    mapping.setKeyType( Mapping.KeyType.DATE );
    decodedValue = new Date( -9223372036854775806L );
    assertEquals( decodedValue, HBaseValueMeta.decodeKeyValue( new byte[] { 1 }, mapping, BYTES_UTIL ) );
    mapping.setKeyType( Mapping.KeyType.INTEGER );
    decodedValue = -2147483645L;
    assertEquals( decodedValue, HBaseValueMeta.decodeKeyValue( new byte[] { 1 }, mapping, BYTES_UTIL ) );
    mapping.setKeyType( Mapping.KeyType.LONG );
    decodedValue = -9223372036854775806L;
    assertEquals( decodedValue, HBaseValueMeta.decodeKeyValue( new byte[] { 1 }, mapping, BYTES_UTIL ) );
    mapping.setKeyType( Mapping.KeyType.UNSIGNED_DATE );
    decodedValue = new Date( 2 );
    assertEquals( decodedValue, HBaseValueMeta.decodeKeyValue( new byte[] { 1 }, mapping, BYTES_UTIL ) );
    mapping.setKeyType( Mapping.KeyType.UNSIGNED_INTEGER );
    decodedValue = 3L;
    assertEquals( decodedValue, HBaseValueMeta.decodeKeyValue( new byte[] { 1 }, mapping, BYTES_UTIL ) );
    mapping.setKeyType( Mapping.KeyType.UNSIGNED_LONG );
    decodedValue = 2L;
    assertEquals( decodedValue, HBaseValueMeta.decodeKeyValue( new byte[] { 1 }, mapping, BYTES_UTIL ) );
  }

  @Test
  public void testEncodeColumnValue() throws Exception {
    HBaseValueMeta hbMeta = getHBaseValueMeta();
    hbMeta.setType( 2 );
    Object keyValue = "1";
    assertArrayEquals( new byte[] { 1 },
      HBaseValueMeta.encodeColumnValue( keyValue, new ValueMetaString(), hbMeta, BYTES_UTIL ) );
    hbMeta.setType( 5 );
    hbMeta.setIsLongOrDouble( false );
    keyValue = 1L;
    assertArrayEquals( new byte[] { 2 },
      HBaseValueMeta.encodeColumnValue( keyValue, new ValueMetaInteger(), hbMeta, BYTES_UTIL ) );
    hbMeta.setIsLongOrDouble( true );
    keyValue = 1L;
    assertArrayEquals( new byte[] { 3 },
      HBaseValueMeta.encodeColumnValue( keyValue, new ValueMetaInteger(), hbMeta, BYTES_UTIL ) );
    hbMeta.setType( 1 );
    hbMeta.setIsLongOrDouble( false );
    keyValue = 1.0;
    assertArrayEquals( new byte[] { 4 },
      HBaseValueMeta.encodeColumnValue( keyValue, new ValueMetaNumber(), hbMeta, BYTES_UTIL ) );
    hbMeta.setIsLongOrDouble( true );
    keyValue = 1.0;
    assertArrayEquals( new byte[] { 5 },
      HBaseValueMeta.encodeColumnValue( keyValue, new ValueMetaNumber(), hbMeta, BYTES_UTIL ) );
    hbMeta.setType( 3 );
    keyValue = new Date();
    assertArrayEquals( new byte[] { 3 },
      HBaseValueMeta.encodeColumnValue( keyValue, new ValueMetaDate(), hbMeta, BYTES_UTIL ) );
    hbMeta.setType( 4 );
    keyValue = true;
    assertArrayEquals( new byte[] { 1 },
      HBaseValueMeta.encodeColumnValue( keyValue, new ValueMetaBoolean(), hbMeta, BYTES_UTIL ) );
    hbMeta.setType( 6 );
    keyValue = new BigDecimal( 1 );
    assertArrayEquals( new byte[] { 1 },
      HBaseValueMeta.encodeColumnValue( keyValue, new ValueMetaBigNumber(), hbMeta, BYTES_UTIL ) );
    hbMeta.setType( 7 );
    keyValue = "";
    assertArrayEquals( new byte[] { -84, -19, 0, 5, 116, 0, 0 },
      HBaseValueMeta.encodeColumnValue( keyValue, new ValueMetaSerializable(), hbMeta, BYTES_UTIL ) );
    hbMeta.setType( 8 );
    keyValue = new byte[] { 1 };
    assertArrayEquals( new byte[] { 1 },
      HBaseValueMeta.encodeColumnValue( keyValue, new ValueMetaBinary(), hbMeta, BYTES_UTIL ) );
  }

  @Test
  public void testDecodeColumnValue() throws Exception {
    HBaseValueMeta hbMeta = getHBaseValueMeta();
    hbMeta.setType( 2 );
    Object keyValue = "1";
    assertEquals( keyValue, HBaseValueMeta.decodeColumnValue( new byte[] { 1 }, hbMeta, BYTES_UTIL ) );
    hbMeta.setType( 5 );
    hbMeta.setIsLongOrDouble( false );
    keyValue = 3L;
    assertEquals( keyValue, HBaseValueMeta.decodeColumnValue( new byte[] { 1, 1, 1 }, hbMeta, BYTES_UTIL ) );
    hbMeta.setIsLongOrDouble( true );
    keyValue = 2L;
    assertEquals( keyValue, HBaseValueMeta.decodeColumnValue( new byte[] { 1, 1, 1, 1 }, hbMeta, BYTES_UTIL ) );
    hbMeta.setType( 1 );
    hbMeta.setIsLongOrDouble( false );
    keyValue = 4.0;
    assertEquals( keyValue, HBaseValueMeta.decodeColumnValue( new byte[] { 1 }, hbMeta, BYTES_UTIL ) );
    hbMeta.setIsLongOrDouble( true );
    keyValue = 5.0;
    assertEquals( keyValue, HBaseValueMeta.decodeColumnValue( new byte[] { 1, 1 }, hbMeta, BYTES_UTIL ) );
    hbMeta.setType( 3 );
    keyValue = new Date( 2 );
    assertEquals( keyValue, HBaseValueMeta.decodeColumnValue( new byte[] { 1, 1, 1, 1 }, hbMeta, BYTES_UTIL ) );
    hbMeta.setType( 4 );
    keyValue = true;
    assertEquals( keyValue, HBaseValueMeta.decodeColumnValue( new byte[] { 1 }, hbMeta, BYTES_UTIL ) );
    hbMeta.setType( 6 );
    keyValue = new BigDecimal( 1 );
    assertEquals( keyValue, HBaseValueMeta.decodeColumnValue( new byte[] { 1 }, hbMeta, BYTES_UTIL ) );
    hbMeta.setType( 7 );
    keyValue = "";
    assertEquals( keyValue,
      HBaseValueMeta.decodeColumnValue( new byte[] { -84, -19, 0, 5, 116, 0, 0 }, hbMeta, BYTES_UTIL ) );
    hbMeta.setType( 8 );
    keyValue = new byte[] { 1 };
    assertEquals( keyValue, HBaseValueMeta.decodeColumnValue( (byte[]) keyValue, hbMeta, BYTES_UTIL ) );
  }

  @Test
  public void testDecodeObject() throws Exception {
    assertEquals( null, HBaseValueMeta.decodeObject( null ) );
    assertEquals( null, HBaseValueMeta.decodeObject( new byte[] { 1 } ) );
  }

  @Test
  public void testDecodeBigDecimal() throws Exception {
    assertEquals( new BigDecimal( 1 ), HBaseValueMeta.decodeBigDecimal( null, BYTES_UTIL ) );
    assertEquals( new BigDecimal( 1 ), HBaseValueMeta.decodeBigDecimal( new byte[] { 1 }, BYTES_UTIL ) );
  }

  @Test
  public void testEncodeObject() throws Exception {
    assertArrayEquals( new byte[] { -84, -19, 0, 5, 112 }, HBaseValueMeta.encodeObject( null ) );
    assertArrayEquals( new byte[] { -84, -19, 0, 5, 116, 0, 0 }, HBaseValueMeta.encodeObject( "" ) );
  }

  @Test
  public void testEncodeBigDecimal() throws Exception {
    assertArrayEquals( new byte[] { -84, -19, 0, 5, 112 }, HBaseValueMeta.encodeBigDecimal( null ) );
    assertEquals( 290, HBaseValueMeta.encodeBigDecimal( new BigDecimal( 0 ) ).length );
  }

  @Test
  public void testDecodeBoolFromString() throws Exception {
    assertEquals( true, HBaseValueMeta.decodeBoolFromString( null, BYTES_UTIL ) );
    assertEquals( true, HBaseValueMeta.decodeBoolFromString( new byte[] { 1 }, BYTES_UTIL ) );
  }

  @Test
  public void testDecodeBoolFromNumber() throws Exception {
    assertEquals( null, HBaseValueMeta.decodeBoolFromNumber( null, BYTES_UTIL ) );
    assertEquals( null, HBaseValueMeta.decodeBoolFromNumber( new byte[] { 1 }, BYTES_UTIL ) );
  }

  @Test
  public void testStringIndexListToObjects() throws Exception {
    assertArrayEquals( null, HBaseValueMeta.stringIndexListToObjects( null ) );
    assertArrayEquals( new Object[] { "1", "2", "3" }, HBaseValueMeta.stringIndexListToObjects( "1,2,3" ) );
    assertArrayEquals( new Object[] { "1", "2", "3" }, HBaseValueMeta.stringIndexListToObjects( "{1},}2{,3" ) );
  }

  @Test
  public void testObjectIndexValuesToString() throws Exception {
    assertEquals( "{}", HBaseValueMeta.objectIndexValuesToString( null ) );
    assertEquals( "{1,2,3}", HBaseValueMeta.objectIndexValuesToString( new Object[] { "1", "2", "3" } ) );
  }
}

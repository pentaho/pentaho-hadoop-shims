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


package org.pentaho.hadoop.shim.api.internal.hbase;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBigNumber;
import org.pentaho.di.core.row.value.ValueMetaBinary;
import org.pentaho.di.core.row.value.ValueMetaBoolean;
import org.pentaho.di.core.row.value.ValueMetaDate;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaNumber;
import org.pentaho.di.core.row.value.ValueMetaSerializable;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.hadoop.shim.spi.MockHBaseByteConverterUsingJavaByteBuffer;
import org.pentaho.hadoop.shim.spi.MockHBaseBytesUtilShim;

import static org.mockito.Mockito.mock;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Date;

import static org.junit.Assert.*;

/**
 * User: Dzmitry Stsiapanau Date: 10/16/2015 Time: 08:38
 */

public class HBaseValueMetaTest extends HBaseValueMeta {

  /**
   * the byte array with size not covered test methods in {@link MockHBaseBytesUtilShim} that allows to get size of the
   * byte arrays. E.g. {@link MockHBaseBytesUtilShim#getSizeOfShort()}
   */
  byte[] ENCODED_INCORRECT_BYTE_ARRAY = new byte[] { 1, 1, 1, 1, 1, 1, 1, 1 };
  private static final String DEF_NAME = "col_family,col_name,alias";
  private static final String DEF_NAME_NO_COL_NAME = "col_family";
  private static final String DEF_NAME_EXTRA_PARAM = "col_family,col_name,alias,extra_argument";
  private static final int DEF_TYPE = 0;
  private static final int DEF_LENGTH = 0;
  private static final int DEF_PRECISION = 0;
  public static final HBaseBytesUtilShim BYTES_UTIL_MOCK = mock( HBaseBytesUtilShim.class );
  public static final MockHBaseBytesUtilShim BYTES_UTIL = new MockHBaseBytesUtilShim();
  public static final MockHBaseByteConverterUsingJavaByteBuffer BYTE_BUFFER_UTIL =
    new MockHBaseByteConverterUsingJavaByteBuffer();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

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
  public void testConstructWithNotEnoughParams() {
    thrown.expect( IllegalArgumentException.class );
    new HBaseValueMeta( DEF_NAME_NO_COL_NAME, DEF_TYPE, DEF_LENGTH, DEF_PRECISION );
  }

  @Test
  public void testConstructWithExtraParams() {
    thrown.expect( IllegalArgumentException.class );
    new HBaseValueMeta( DEF_NAME_EXTRA_PARAM, DEF_TYPE, DEF_LENGTH, DEF_PRECISION );
  }

  @Test
  public void testAliasParams() {
    HBaseValueMeta hBaseValueMeta = getHBaseValueMeta();
    assertEquals( hBaseValueMeta.getAlias(), "alias" );
  }

  @Test
  public void testValueTypeNotFound() {
    HBaseValueMeta hBaseValueMeta = getHBaseValueMeta();
    thrown.expect( IllegalArgumentException.class );
    hBaseValueMeta.setHBaseTypeFromString( "NO SUCH TYPE" );
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
      encodeKeyValue( keyValue, new ValueMetaString(), Mapping.KeyType.STRING, BYTES_UTIL ) );
    keyValue = new Date();
    assertArrayEquals( new byte[] { 3 },
      encodeKeyValue( keyValue, new ValueMetaDate(), Mapping.KeyType.DATE, BYTES_UTIL ) );
    keyValue = new Date();
    assertArrayEquals( new byte[] { 3 },
      encodeKeyValue( keyValue, new ValueMetaDate(), Mapping.KeyType.UNSIGNED_DATE, BYTES_UTIL ) );
    keyValue = (long) 0;
    assertArrayEquals( new byte[] { 2 },
      encodeKeyValue( keyValue, new ValueMetaInteger(), Mapping.KeyType.INTEGER, BYTES_UTIL ) );
    keyValue = (long) 0;
    assertArrayEquals( new byte[] { 2 },
      encodeKeyValue( keyValue, new ValueMetaInteger(), Mapping.KeyType.UNSIGNED_INTEGER, BYTES_UTIL ) );
    keyValue = (double) 0;
    assertArrayEquals( new byte[] { 3 },
      encodeKeyValue( keyValue, new ValueMetaNumber(), Mapping.KeyType.LONG, BYTES_UTIL ) );
    keyValue = (double) 0;
    assertArrayEquals( new byte[] { 3 },
      encodeKeyValue( keyValue, new ValueMetaNumber(), Mapping.KeyType.UNSIGNED_LONG, BYTES_UTIL ) );
    keyValue = new byte[] { 3 };
    assertArrayEquals( (byte[]) keyValue,
      encodeKeyValue( keyValue, new ValueMetaBinary(), Mapping.KeyType.BINARY, BYTES_UTIL ) );
  }

  @Test
  public void testEncodeKeyValueThrowExceptionOnNegativeForUnsignedInteger() throws Exception {
    Object keyValue = -1;
    thrown.expect( KettleException.class );
    encodeKeyValue( keyValue, new ValueMetaInteger(), Mapping.KeyType.UNSIGNED_INTEGER, BYTE_BUFFER_UTIL );
  }

  @Test
  public void testEncodeKeyValueThrowExceptionOnNegativeForUnsignedLong() throws Exception {
    Object keyValue = -1;
    thrown.expect( KettleException.class );
    encodeKeyValue( keyValue, new ValueMetaNumber(), Mapping.KeyType.UNSIGNED_LONG, BYTE_BUFFER_UTIL );
  }

  @Test
  public void testEncodeKeyValueThrowExceptionOnNegativeForUnsignedDate() throws Exception {
    Object keyValue = new Date( -1 );
    thrown.expect( KettleException.class );
    encodeKeyValue( keyValue, new ValueMetaDate(), Mapping.KeyType.UNSIGNED_DATE, BYTE_BUFFER_UTIL );
  }

  @Test
  public void testEncodeKey2ValueThrowExceptionOnNegativeForUnsignedInteger() throws Exception {
    Object keyValue = -1;
    thrown.expect( KettleException.class );
    encodeKeyValue( keyValue, Mapping.KeyType.UNSIGNED_INTEGER, BYTE_BUFFER_UTIL );
  }

  @Test
  public void testEncodeKey2ValueThrowExceptionOnNegativeForUnsignedLong() throws Exception {
    Object keyValue = -1;
    thrown.expect( KettleException.class );
    encodeKeyValue( keyValue, Mapping.KeyType.UNSIGNED_LONG, BYTE_BUFFER_UTIL );
  }

  @Test
  public void testEncodeKey2ValueThrowExceptionOnNegativeForUnsignedDate() throws Exception {
    Object keyValue = new Date( -1 );
    thrown.expect( KettleException.class );
    encodeKeyValue( keyValue, Mapping.KeyType.UNSIGNED_DATE, BYTE_BUFFER_UTIL );
  }

  @Test
  public void testEncodeKeyValue() throws Exception {
    HBaseValueMeta hbMeta = getHBaseValueMeta();
    Object keyValue = "1";
    assertArrayEquals( new byte[] { 1 },
      encodeKeyValue( keyValue, Mapping.KeyType.STRING, BYTES_UTIL ) );
    keyValue = new Date();
    assertArrayEquals( new byte[] { 3 },
      encodeKeyValue( keyValue, Mapping.KeyType.DATE, BYTES_UTIL ) );
    keyValue = new Date();
    assertArrayEquals( new byte[] { 3 },
      encodeKeyValue( keyValue, Mapping.KeyType.UNSIGNED_DATE, BYTES_UTIL ) );
    keyValue = 0;
    assertArrayEquals( new byte[] { 2 },
      encodeKeyValue( keyValue, Mapping.KeyType.INTEGER, BYTES_UTIL ) );
    keyValue = 0;
    assertArrayEquals( new byte[] { 2 },
      encodeKeyValue( keyValue, Mapping.KeyType.UNSIGNED_INTEGER, BYTES_UTIL ) );
    keyValue = (long) 0;
    assertArrayEquals( new byte[] { 3 },
      encodeKeyValue( keyValue, Mapping.KeyType.LONG, BYTES_UTIL ) );
    keyValue = (long) 0;
    assertArrayEquals( new byte[] { 3 },
      encodeKeyValue( keyValue, Mapping.KeyType.UNSIGNED_LONG, BYTES_UTIL ) );
    keyValue = new byte[] { 3 };
    assertArrayEquals( (byte[]) keyValue,
      encodeKeyValue( keyValue, Mapping.KeyType.BINARY, BYTES_UTIL ) );
  }

  @Test
  public void testEncodeKeyValueWithString() throws Exception {
    HBaseValueMeta hbMeta = getHBaseValueMeta();
    String keyValue = "1";
    assertArrayEquals( new byte[] { 1 },
      encodeKeyValue( keyValue, Mapping.KeyType.STRING, BYTES_UTIL ) );
    keyValue = new Date().toString();
    try {
      encodeKeyValue( keyValue, Mapping.KeyType.DATE, BYTES_UTIL );
      fail( "For date should be exception" );
    } catch ( KettleException e ) {
      //expected
    }
    keyValue = new Date().toString();
    try {
      encodeKeyValue( keyValue, Mapping.KeyType.UNSIGNED_DATE, BYTES_UTIL );
      fail( "For date should be exception" );
    } catch ( KettleException e ) {
      //expected
    }
    keyValue = Integer.toString( 0 );
    assertArrayEquals( new byte[] { 2 },
      encodeKeyValue( keyValue, Mapping.KeyType.INTEGER, BYTES_UTIL ) );
    keyValue = Integer.toString( 0 );
    assertArrayEquals( new byte[] { 2 },
      encodeKeyValue( keyValue, Mapping.KeyType.UNSIGNED_INTEGER, BYTES_UTIL ) );
    keyValue = Long.toString( 0 );
    assertArrayEquals( new byte[] { 3 },
      encodeKeyValue( keyValue, Mapping.KeyType.LONG, BYTES_UTIL ) );
    keyValue = Long.toString( 0 );
    assertArrayEquals( new byte[] { 3 },
      encodeKeyValue( keyValue, Mapping.KeyType.UNSIGNED_LONG, BYTES_UTIL ) );
    keyValue = new Object().toString();
    assertArrayEquals( new byte[] { 6 },
      encodeKeyValue( keyValue, Mapping.KeyType.BINARY, BYTES_UTIL ) );
  }

  @Test
  public void testDecodeKeyValue() throws Exception {
    HBaseValueMeta hbMeta = getHBaseValueMeta();
    Mapping mapping = new Mapping();
    mapping.setKeyType( Mapping.KeyType.STRING );
    assertEquals( null, decodeKeyValue( null, mapping, BYTES_UTIL ) );
    Object decodedValue = "1";
    assertEquals( decodedValue, decodeKeyValue( new byte[] { 1 }, mapping, BYTES_UTIL ) );
    mapping.setKeyType( Mapping.KeyType.BINARY );
    decodedValue = new byte[] { 1 };
    assertEquals( decodedValue, decodeKeyValue( (byte[]) decodedValue, mapping, BYTES_UTIL ) );
    mapping.setKeyType( Mapping.KeyType.DATE );
    decodedValue = new Date( -9223372036854775806L );
    assertEquals( decodedValue, decodeKeyValue( new byte[] { 1 }, mapping, BYTES_UTIL ) );
    mapping.setKeyType( Mapping.KeyType.INTEGER );
    decodedValue = -2147483645L;
    assertEquals( decodedValue, decodeKeyValue( new byte[] { 1 }, mapping, BYTES_UTIL ) );
    mapping.setKeyType( Mapping.KeyType.LONG );
    decodedValue = -9223372036854775806L;
    assertEquals( decodedValue, decodeKeyValue( new byte[] { 1 }, mapping, BYTES_UTIL ) );
    mapping.setKeyType( Mapping.KeyType.UNSIGNED_DATE );
    decodedValue = new Date( 2 );
    assertEquals( decodedValue, decodeKeyValue( new byte[] { 1 }, mapping, BYTES_UTIL ) );
    mapping.setKeyType( Mapping.KeyType.UNSIGNED_INTEGER );
    decodedValue = 3L;
    assertEquals( decodedValue, decodeKeyValue( new byte[] { 1 }, mapping, BYTES_UTIL ) );
    mapping.setKeyType( Mapping.KeyType.UNSIGNED_LONG );
    decodedValue = 2L;
    assertEquals( decodedValue, decodeKeyValue( new byte[] { 1 }, mapping, BYTES_UTIL ) );
  }

  @Test
  public void testEncodeColumnValue() throws Exception {
    HBaseValueMeta hbMeta = getHBaseValueMeta();
    hbMeta.setType( 2 );
    Object keyValue = "1";
    assertArrayEquals( new byte[] { 1 },
      encodeColumnValue( keyValue, new ValueMetaString(), hbMeta, BYTES_UTIL ) );
    hbMeta.setType( 5 );
    hbMeta.setIsLongOrDouble( false );
    keyValue = 1L;
    assertArrayEquals( new byte[] { 2 },
      encodeColumnValue( keyValue, new ValueMetaInteger(), hbMeta, BYTES_UTIL ) );
    hbMeta.setIsLongOrDouble( true );
    keyValue = 1L;
    assertArrayEquals( new byte[] { 3 },
      encodeColumnValue( keyValue, new ValueMetaInteger(), hbMeta, BYTES_UTIL ) );
    hbMeta.setType( 1 );
    hbMeta.setIsLongOrDouble( false );
    keyValue = 1.0;
    assertArrayEquals( new byte[] { 4 },
      encodeColumnValue( keyValue, new ValueMetaNumber(), hbMeta, BYTES_UTIL ) );
    hbMeta.setIsLongOrDouble( true );
    keyValue = 1.0;
    assertArrayEquals( new byte[] { 5 },
      encodeColumnValue( keyValue, new ValueMetaNumber(), hbMeta, BYTES_UTIL ) );
    hbMeta.setType( 3 );
    keyValue = new Date();
    assertArrayEquals( new byte[] { 3 },
      encodeColumnValue( keyValue, new ValueMetaDate(), hbMeta, BYTES_UTIL ) );
    hbMeta.setType( 4 );
    keyValue = true;
    assertArrayEquals( new byte[] { 1 },
      encodeColumnValue( keyValue, new ValueMetaBoolean(), hbMeta, BYTES_UTIL ) );
    hbMeta.setType( 6 );
    keyValue = new BigDecimal( 1 );
    assertArrayEquals( new byte[] { 1 },
      encodeColumnValue( keyValue, new ValueMetaBigNumber(), hbMeta, BYTES_UTIL ) );
    hbMeta.setType( 7 );
    keyValue = "";
    assertArrayEquals( new byte[] { -84, -19, 0, 5, 116, 0, 0 },
      encodeColumnValue( keyValue, new ValueMetaSerializable(), hbMeta, BYTES_UTIL ) );
    hbMeta.setType( 8 );
    keyValue = new byte[] { 1 };
    assertArrayEquals( new byte[] { 1 },
      encodeColumnValue( keyValue, new ValueMetaBinary(), hbMeta, BYTES_UTIL ) );
  }

  @Test
  public void testDecodeColumnValueIndexedString() throws Exception {
    //what is happening in section when storage type indexed set??
    HBaseValueMeta hbMeta = getHBaseValueMeta();
    hbMeta.setType( 2 );
    String indexedStorageString = "   Indexed string";
    hbMeta.setStorageType( STORAGE_TYPE_INDEXED );
    hbMeta.setIndex( new Object[] { "Indexed string" } );
    assertEquals( 0,
      decodeColumnValue( indexedStorageString.getBytes(), hbMeta, BYTE_BUFFER_UTIL ) );
  }

  @Test
  public void testDecodeColumnValueStringIndexedStorageIllegalValue() {
    String value = "Bogus";
    byte[] encoded = BYTES_UTIL.toBytes( value );

    HBaseValueMeta mappingMeta =
      new HBaseValueMeta( "famliy1" + SEPARATOR + "testcol" + SEPARATOR + "anAlias", TYPE_STRING, -1, -1 );
    mappingMeta.setStorageType( STORAGE_TYPE_INDEXED );
    Object[] legalVals = new Object[] { "Value1", "Value2", "Value3" };
    mappingMeta.setIndex( legalVals );

    try {
      decodeColumnValue( encoded, mappingMeta, BYTES_UTIL );
      fail( "Should have thrown an exception because the supplied value is not in the list of indexed values" );
    } catch ( Exception ex ) {
      assertEquals( "Test for IllegalIndexedColumnValue exception",
        "Value \"1\" is not in the list of legal values for indexed column \"anAlias\"", ex.getMessage().trim() );
    }
  }

  @Test
  public void testDecodeColumnValue() throws Exception {
    HBaseValueMeta hbMeta = getHBaseValueMeta();
    hbMeta.setType( 2 );
    assertEquals( null, decodeColumnValue( null, hbMeta, BYTES_UTIL ) );
    Object keyValue = "1";
    assertEquals( keyValue, decodeColumnValue( new byte[] { 1 }, hbMeta, BYTES_UTIL ) );
    hbMeta.setType( 5 );
    hbMeta.setIsLongOrDouble( false );
    keyValue = 3L;
    assertEquals( keyValue, decodeColumnValue( new byte[] { 1, 1, 1 }, hbMeta, BYTES_UTIL ) );
    keyValue = (short) 6;
    assertEquals( Long.valueOf( keyValue.toString() ),
      decodeColumnValue( new byte[] { 1, 1, 1, 1, 1 }, hbMeta, BYTES_UTIL ) );
    hbMeta.setIsLongOrDouble( true );
    keyValue = 2L;
    assertEquals( keyValue, decodeColumnValue( new byte[] { 1, 1, 1, 1 }, hbMeta, BYTES_UTIL ) );
    hbMeta.setType( 1 );
    hbMeta.setIsLongOrDouble( false );
    keyValue = 4.0;
    assertEquals( keyValue, decodeColumnValue( new byte[] { 1 }, hbMeta, BYTES_UTIL ) );
    hbMeta.setIsLongOrDouble( true );
    keyValue = 5.0;
    assertEquals( keyValue, decodeColumnValue( new byte[] { 1, 1 }, hbMeta, BYTES_UTIL ) );
    hbMeta.setType( 3 );
    keyValue = new Date( 2 );
    assertEquals( keyValue, decodeColumnValue( new byte[] { 1, 1, 1, 1 }, hbMeta, BYTES_UTIL ) );
    hbMeta.setType( 4 );
    keyValue = true;
    assertEquals( keyValue, decodeColumnValue( new byte[] { 1 }, hbMeta, BYTES_UTIL ) );
    hbMeta.setType( 6 );
    keyValue = new BigDecimal( 1 );
    assertEquals( keyValue, decodeColumnValue( new byte[] { 1 }, hbMeta, BYTES_UTIL ) );
    hbMeta.setType( 7 );
    keyValue = "";
    assertEquals( keyValue, decodeColumnValue( new byte[] { -84, -19, 0, 5, 116, 0, 0 }, hbMeta, BYTES_UTIL ) );
    hbMeta.setType( 8 );
    keyValue = new byte[] { 1 };
    assertEquals( keyValue, decodeColumnValue( (byte[]) keyValue, hbMeta, BYTES_UTIL ) );
  }

  @Test
  public void testDecodeObject() throws Exception {
    assertEquals( null, decodeObject( null ) );
    assertEquals( null, decodeObject( new byte[] { 1 } ) );
  }

  @Test
  public void testDecodeBigDecimal() throws Exception {
    assertEquals( new BigDecimal( 1 ), decodeBigDecimal( null, BYTES_UTIL ) );
    assertEquals( new BigDecimal( 1 ), decodeBigDecimal( new byte[] { 1 }, BYTES_UTIL ) );
  }

  @Test
  public void testDecodeBigDecimalSerialized() throws Exception {
    try ( ByteArrayOutputStream bos = new ByteArrayOutputStream() ) {
      ObjectOutput out = new ObjectOutputStream( bos );
      int value = 123;
      out.writeObject( new BigDecimal( value ) );
      assertEquals( new BigDecimal( value ), decodeBigDecimal( bos.toByteArray(), BYTE_BUFFER_UTIL ) );
    }
  }

  @Test
  public void testDecodeColumnValueNumber_ThrownUnknownTypeForColumnException() throws Exception {
    HBaseValueMeta hbMeta = getHBaseValueMeta();
    // Number type
    hbMeta.setType( 1 );
    try {
      decodeColumnValue( ENCODED_INCORRECT_BYTE_ARRAY, hbMeta, BYTES_UTIL );
      fail(
        "Should have thrown an exception because the ValueMetaInterface.TYPE_NUMBER should be either Float or Double" );
    } catch ( Exception ex ) {
      assertTrue( ex instanceof KettleException );
      assertEquals( "Test for UnknownTypeForColumn exception", "Unknown type for column", ex.getMessage().trim() );
    }
  }

  @Test
  public void testDecodeColumnValueInteger_ThrownIllegalIntegerLengthException() throws Exception {
    HBaseValueMeta hbMeta = getHBaseValueMeta();
    // Integer type
    hbMeta.setType( 5 );
    try {
      decodeColumnValue( ENCODED_INCORRECT_BYTE_ARRAY, hbMeta, BYTES_UTIL );
      fail( "Should have thrown an KettleException" );
    } catch ( Exception ex ) {
      assertTrue( ex instanceof KettleException );
      assertEquals( "Test for IllegalIntegerLength exception",
        "Length of integer column value is not equal to the defined length of a short, int or long",
        ex.getMessage().trim() );
    }
  }

  @Test
  public void testDecodeColumnValueBoolean_ThrownIllegalIntegerLengthException() throws Exception {
    HBaseValueMeta hbMeta = getHBaseValueMeta();
    // Boolean type
    hbMeta.setType( 4 );
    byte[] anyStringBytes = "ANY STRING".getBytes();
    try {
      decodeColumnValue( anyStringBytes, hbMeta, BYTE_BUFFER_UTIL );
      fail( "Should have thrown an KettleException" );
    } catch ( Exception ex ) {
      assertTrue( ex instanceof KettleException );
      assertEquals( "Test for UnableToDecodeBoolean exception", "Unable to decode boolean value",
        ex.getMessage().trim() );
    }
  }

  @Test
  public void testEncodeObject() throws Exception {
    assertArrayEquals( new byte[] { -84, -19, 0, 5, 112 }, encodeObject( null ) );
    assertArrayEquals( new byte[] { -84, -19, 0, 5, 116, 0, 0 }, encodeObject( "" ) );
  }

  @Test
  public void testEncodeBigDecimal() throws Exception {
    assertArrayEquals( new byte[] { -84, -19, 0, 5, 112 }, encodeBigDecimal( null ) );
    assertEquals( 290, encodeBigDecimal( new BigDecimal( 0 ) ).length );
  }

  @Test
  public void testDecodeBoolFromString() throws Exception {
    assertEquals( true, decodeBoolFromString( null, BYTES_UTIL ) );
    assertEquals( true, decodeBoolFromString( new byte[] { 1 }, BYTES_UTIL ) );
  }

  @Test
  public void testDecodeBoolFromStringAgainstByteBuffer() throws Exception {
    byte[] yBytes = "Y".getBytes();
    assertEquals( true, decodeBoolFromString( yBytes, BYTE_BUFFER_UTIL ) );
    byte[] tBytes = "T".getBytes();
    assertEquals( true, decodeBoolFromString( tBytes, BYTE_BUFFER_UTIL ) );
    byte[] oneBytes = "1".getBytes();
    assertEquals( true, decodeBoolFromString( oneBytes, BYTE_BUFFER_UTIL ) );
    byte[] yesBytes = "YES".getBytes();
    assertEquals( true, decodeBoolFromString( yesBytes, BYTE_BUFFER_UTIL ) );
    byte[] trueBytes = "TRUE".getBytes();
    assertEquals( true, decodeBoolFromString( trueBytes, BYTE_BUFFER_UTIL ) );
    byte[] nBytes = "N".getBytes();
    assertEquals( false, decodeBoolFromString( nBytes, BYTE_BUFFER_UTIL ) );
    byte[] noBytes = "NO".getBytes();
    assertEquals( false, decodeBoolFromString( noBytes, BYTE_BUFFER_UTIL ) );
    byte[] falseBytes = "FALSE".getBytes();
    assertEquals( false, decodeBoolFromString( falseBytes, BYTE_BUFFER_UTIL ) );
    byte[] fBytes = "F".getBytes();
    assertEquals( false, decodeBoolFromString( fBytes, BYTE_BUFFER_UTIL ) );
    byte[] zeroBytes = "0".getBytes();
    assertEquals( false, decodeBoolFromString( zeroBytes, BYTE_BUFFER_UTIL ) );
    byte[] anyStringBytes = "ANY STRING".getBytes();
    assertEquals( null, decodeBoolFromString( anyStringBytes, BYTE_BUFFER_UTIL ) );
  }

  @Test
  public void testDecodeBoolFromNumber() throws Exception {
    assertEquals( null, decodeBoolFromNumber( null, BYTES_UTIL ) );
    assertEquals( null, decodeBoolFromNumber( new byte[] { 1 }, BYTES_UTIL ) );
    assertEquals( true, decodeBoolFromNumber( new byte[] { 1, 0, 0, 0, 0, 0 }, BYTES_UTIL ) );
  }

  @Test
  public void testDecodeBoolAgainstJavaByteBuffer() throws Exception {
    assertEquals( null, decodeBoolFromNumber( null, BYTE_BUFFER_UTIL ) );
    assertEquals( true, decodeBoolFromNumber( new byte[] { 1 }, BYTE_BUFFER_UTIL ) );
    assertEquals( false, decodeBoolFromNumber( new byte[] { 0 }, BYTE_BUFFER_UTIL ) );
    assertEquals( true, decodeBoolFromNumber( new byte[] { 0, 1 }, BYTE_BUFFER_UTIL ) );
    assertEquals( null, decodeBoolFromNumber( new byte[] { 0, 2 }, BYTE_BUFFER_UTIL ) );
    assertEquals( false, decodeBoolFromNumber( new byte[] { 0, 0 }, BYTE_BUFFER_UTIL ) );
    assertEquals( true, decodeBoolFromNumber( new byte[] { 0, 0, 0, 1 }, BYTE_BUFFER_UTIL ) );
    assertEquals( false, decodeBoolFromNumber( new byte[] { 0, 0, 0, 0 }, BYTE_BUFFER_UTIL ) );
    assertEquals( null, decodeBoolFromNumber( new byte[] { 0, 0, 0, 2 }, BYTE_BUFFER_UTIL ) );
    assertEquals( true, decodeBoolFromNumber( new byte[] { 0, 0, 0, 0, 0, 0, 0, 1 }, BYTE_BUFFER_UTIL ) );
    assertEquals( false, decodeBoolFromNumber( new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 }, BYTE_BUFFER_UTIL ) );
    assertEquals( null, decodeBoolFromNumber( new byte[] { 0, 0, 2, 0, 0, 0, 0, 2 }, BYTE_BUFFER_UTIL ) );

    byte[] double2 = ByteBuffer.allocate( BYTE_BUFFER_UTIL.getSizeOfDouble() ).putDouble( 2 ).array();
    assertEquals( null, decodeBoolFromNumber( double2, BYTE_BUFFER_UTIL ) );
    byte[] double0 = ByteBuffer.allocate( BYTE_BUFFER_UTIL.getSizeOfDouble() ).putDouble( 0 ).array();
    assertEquals( false, decodeBoolFromNumber( double0, BYTE_BUFFER_UTIL ) );
    byte[] double1 = ByteBuffer.allocate( BYTE_BUFFER_UTIL.getSizeOfDouble() ).putDouble( 1 ).array();
    assertEquals( true, decodeBoolFromNumber( double1, BYTE_BUFFER_UTIL ) );

    byte[] float2 = ByteBuffer.allocate( BYTE_BUFFER_UTIL.getSizeOfFloat() ).putFloat( 2 ).array();
    assertEquals( null, decodeBoolFromNumber( float2, BYTE_BUFFER_UTIL ) );
    byte[] float0 = ByteBuffer.allocate( BYTE_BUFFER_UTIL.getSizeOfFloat() ).putFloat( 0 ).array();
    assertEquals( false, decodeBoolFromNumber( float0, BYTE_BUFFER_UTIL ) );
    byte[] float1 = ByteBuffer.allocate( BYTE_BUFFER_UTIL.getSizeOfFloat() ).putFloat( 1 ).array();
    assertEquals( true, decodeBoolFromNumber( float1, BYTE_BUFFER_UTIL ) );

  }

  @Test
  public void testStringIndexListToObjects() throws Exception {
    assertArrayEquals( null, stringIndexListToObjects( null ) );
    assertArrayEquals( new Object[] { "1", "2", "3" }, stringIndexListToObjects( "1,2,3" ) );
    assertArrayEquals( new Object[] { "1", "2", "3" }, stringIndexListToObjects( "{1},}2{,3" ) );
  }

  @Test
  public void testObjectIndexValuesToString() throws Exception {
    assertEquals( "{}", objectIndexValuesToString( null ) );
    assertEquals( "{1,2,3}", objectIndexValuesToString( new Object[] { "1", "2", "3" } ) );
  }
}

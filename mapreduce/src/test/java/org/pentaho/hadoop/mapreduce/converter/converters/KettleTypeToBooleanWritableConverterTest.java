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


package org.pentaho.hadoop.mapreduce.converter.converters;

import org.apache.hadoop.io.BooleanWritable;
import org.junit.Test;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.mapreduce.converter.TypeConversionException;

import static org.junit.Assert.*;

public class KettleTypeToBooleanWritableConverterTest {
  @Test
  public void canConvert() throws Exception {
    KettleTypeToBooleanWritableConverter c = new KettleTypeToBooleanWritableConverter();

    assertTrue( c.canConvert( String.class, BooleanWritable.class ) );
    assertTrue( c.canConvert( Long.class, BooleanWritable.class ) );
    assertTrue( c.canConvert( String.class, BooleanWritable.class ) );
    assertFalse( c.canConvert( Object.class, BooleanWritable.class ) );
    assertFalse( c.canConvert( BooleanWritable.class, BooleanWritable.class ) );
    assertFalse( c.canConvert( null, null ) );
    assertFalse( c.canConvert( BooleanWritable.class, Object.class ) );
    assertFalse( c.canConvert( Object.class, Long.class ) );
  }

  @Test
  public void convert() throws Exception {
    KettleTypeToBooleanWritableConverter c = new KettleTypeToBooleanWritableConverter();
    BooleanWritable expected = new BooleanWritable( false );
    String value = "false";

    // Convert from a normal String
    ValueMeta normalMeta =
      new ValueMeta( "test", ValueMetaInterface.TYPE_STRING, ValueMetaInterface.STORAGE_TYPE_NORMAL );
    assertEquals( expected, c.convert( normalMeta, value ) );

    // Convert from a byte array
    ValueMeta binaryMeta =
      new ValueMeta( "test", ValueMetaInterface.TYPE_STRING, ValueMetaInterface.STORAGE_TYPE_BINARY_STRING );
    ValueMeta storageMeta =
      new ValueMeta( "test", ValueMetaInterface.TYPE_STRING, ValueMetaInterface.STORAGE_TYPE_NORMAL );
    binaryMeta.setStorageMetadata( storageMeta );
    byte[] rawValue = value.getBytes( "UTF-8" );
    assertEquals( expected, c.convert( binaryMeta, rawValue ) );

    // Convert from an Integer
    ValueMeta integerMeta =
      new ValueMeta( "test", ValueMetaInterface.TYPE_INTEGER, ValueMetaInterface.STORAGE_TYPE_NORMAL );
    assertEquals( new BooleanWritable( true ), c.convert( integerMeta, Long.valueOf( 1 ) ) );

    try {
      c.convert( null, null );
      fail();
    } catch ( TypeConversionException ex ) {
      assertTrue( ex.getMessage().contains( "!ErrorConverting!" ) );
    }

    try {
      c.convert( integerMeta, "not an integer" );
      fail();
    } catch ( TypeConversionException ex ) {
      assertTrue( ex.getMessage().contains( "!ErrorConverting!" ) );
    }
  }
}

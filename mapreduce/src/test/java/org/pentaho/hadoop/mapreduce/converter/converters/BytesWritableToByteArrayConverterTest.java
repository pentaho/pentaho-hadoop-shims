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

package org.pentaho.hadoop.mapreduce.converter.converters;

import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

import static org.junit.Assert.*;

public class BytesWritableToByteArrayConverterTest {
  @Test
  public void canConvert() throws Exception {
    BytesWritableToByteArrayConverter c = new BytesWritableToByteArrayConverter();

    assertTrue( c.canConvert( BytesWritable.class, byte[].class ) );
    assertFalse( c.canConvert( null, null ) );
    assertFalse( c.canConvert( BytesWritable.class, Object.class ) );
    assertFalse( c.canConvert( Object.class, byte[].class ) );
  }

  @Test
  public void convert() throws Exception {
    BytesWritableToByteArrayConverter c = new BytesWritableToByteArrayConverter();
    byte[] expected = "testing".getBytes();
    byte[] converted = c.convert( null, new BytesWritable( expected ) );

    assertEquals( expected.length, converted.length );
    for ( int i = 0; i < expected.length; i++ ) {
      assertEquals( expected[ i ], converted[ i ] );
    }

    try {
      c.convert( null, null );
      fail();
    } catch ( NullPointerException ex ) {
      // Expected
    }
  }

  @Test
  public void testConvertFreshArray() throws Exception {
    BytesWritableToByteArrayConverter c = new BytesWritableToByteArrayConverter();
    byte[] expected = "testing".getBytes();
    byte[] converted = c.convert( null, new BytesWritable( expected ) );

    // arrays should be two separate objects
    assertTrue( expected != converted );
  }
}

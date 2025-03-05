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

import org.apache.hadoop.io.DoubleWritable;
import org.junit.Test;

import static org.junit.Assert.*;

public class DoubleWritableToLongConverterTest {
  @Test
  public void canConvert() throws Exception {
    DoubleWritableToLongConverter c = new DoubleWritableToLongConverter();

    assertTrue( c.canConvert( DoubleWritable.class, Long.class ) );
    assertFalse( c.canConvert( null, null ) );
    assertFalse( c.canConvert( DoubleWritable.class, Object.class ) );
    assertFalse( c.canConvert( Object.class, Long.class ) );
  }

  @Test
  public void convert() throws Exception {
    DoubleWritableToLongConverter c = new DoubleWritableToLongConverter();
    Long expected = 42L;

    assertEquals( expected, c.convert( null, new DoubleWritable( 42.42 ) ) );

    try {
      c.convert( null, null );
      fail();
    } catch ( NullPointerException ex ) {
      // Expected
    }
  }
}

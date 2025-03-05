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

public class DoubleWritableToDoubleConverterTest {
  @Test
  public void canConvert() throws Exception {
    DoubleWritableToDoubleConverter c = new DoubleWritableToDoubleConverter();

    assertTrue( c.canConvert( DoubleWritable.class, Double.class ) );
    assertFalse( c.canConvert( null, null ) );
    assertFalse( c.canConvert( DoubleWritable.class, Object.class ) );
    assertFalse( c.canConvert( Object.class, Double.class ) );
  }

  @Test
  public void convert() throws Exception {
    DoubleWritableToDoubleConverter c = new DoubleWritableToDoubleConverter();
    Double expected = 42.314;

    assertEquals( expected, c.convert( null, new DoubleWritable( expected ) ) );

    try {
      c.convert( null, null );
      fail();
    } catch ( NullPointerException ex ) {
      // Expected
    }
  }
}

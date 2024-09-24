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

import org.apache.hadoop.io.LongWritable;
import org.junit.Test;

import static org.junit.Assert.*;

public class LongWritableToLongConverterTest {
  @Test
  public void canConvert() throws Exception {
    LongWritableToLongConverter c = new LongWritableToLongConverter();

    assertTrue( c.canConvert( LongWritable.class, Long.class ) );
    assertFalse( c.canConvert( null, null ) );
    assertFalse( c.canConvert( LongWritable.class, Object.class ) );
    assertFalse( c.canConvert( Object.class, Long.class ) );
  }

  @Test
  public void convert() throws Exception {
    LongWritableToLongConverter c = new LongWritableToLongConverter();
    Long expected = 10L;

    assertEquals( expected, c.convert( null, new LongWritable( expected ) ) );

    try {
      c.convert( null, null );
      fail();
    } catch ( NullPointerException ex ) {
      // Expected
    }
  }
}

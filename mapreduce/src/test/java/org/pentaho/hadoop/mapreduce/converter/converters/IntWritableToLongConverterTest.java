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

import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

import static org.junit.Assert.*;

public class IntWritableToLongConverterTest {
  @Test
  public void canConvert() throws Exception {
    IntWritableToLongConverter c = new IntWritableToLongConverter();

    assertTrue( c.canConvert( IntWritable.class, Long.class ) );
    assertFalse( c.canConvert( null, null ) );
    assertFalse( c.canConvert( IntWritable.class, Object.class ) );
    assertFalse( c.canConvert( Object.class, Long.class ) );
  }

  @Test
  public void convert() throws Exception {
    IntWritableToLongConverter c = new IntWritableToLongConverter();
    Long expected = 10L;

    assertEquals( expected, c.convert( null, new IntWritable( expected.intValue() ) ) );

    try {
      c.convert( null, null );
      fail();
    } catch ( NullPointerException ex ) {
      // Expected
    }
  }
}

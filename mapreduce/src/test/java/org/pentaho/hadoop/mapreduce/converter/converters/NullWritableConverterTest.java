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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import static org.junit.Assert.*;

public class NullWritableConverterTest {
  @Test
  public void canConvert() throws Exception {
    NullWritableConverter c = new NullWritableConverter();

    assertTrue( c.canConvert( Object.class, NullWritable.class ) );
    assertTrue( c.canConvert( null, NullWritable.class ) );
    assertFalse( c.canConvert( null, null ) );
    assertFalse( c.canConvert( LongWritable.class, Object.class ) );
    assertFalse( c.canConvert( Object.class, Text.class ) );
  }

  @Test
  public void convert() throws Exception {
    NullWritableConverter c = new NullWritableConverter();
    Long expected = 10L;

    assertEquals( NullWritable.get(), c.convert( null, new LongWritable( expected ) ) );
    assertEquals( NullWritable.get(), c.convert( null, null ) );
  }
}

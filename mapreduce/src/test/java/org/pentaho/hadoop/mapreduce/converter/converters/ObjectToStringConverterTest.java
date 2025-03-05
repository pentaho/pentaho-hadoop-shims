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

import org.apache.hadoop.io.LongWritable;
import org.junit.Test;

import static org.junit.Assert.*;

public class ObjectToStringConverterTest {
  @Test
  public void canConvert() throws Exception {
    ObjectToStringConverter c = new ObjectToStringConverter();

    assertTrue( c.canConvert( Object.class, String.class ) );
    assertTrue( c.canConvert( null, String.class ) );
    assertFalse( c.canConvert( null, null ) );
    assertFalse( c.canConvert( String.class, Object.class ) );
  }

  @Test
  public void convert() throws Exception {
    ObjectToStringConverter c = new ObjectToStringConverter();
    String expected = "10";

    assertEquals( expected, c.convert( null, new LongWritable( 10L ) ) );
    assertEquals( expected, c.convert( null, new LongWritable( 10L ) ) );
    assertEquals( expected, c.convert( null, 10L ) );

    try {
      c.convert( null, null );
      fail();
    } catch ( NullPointerException ex ) {
      // Expected
    }
  }
}

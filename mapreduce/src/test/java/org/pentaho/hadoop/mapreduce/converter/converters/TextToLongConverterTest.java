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

import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.pentaho.hadoop.mapreduce.converter.TypeConversionException;

import static org.junit.Assert.*;

public class TextToLongConverterTest {
  @Test
  public void canConvert() throws Exception {
    TextToLongConverter c = new TextToLongConverter();

    assertTrue( c.canConvert( Text.class, Long.class ) );
    assertFalse( c.canConvert( Long.class, Text.class ) );
    assertFalse( c.canConvert( null, null ) );
    assertFalse( c.canConvert( Text.class, Object.class ) );
    assertFalse( c.canConvert( Object.class, Long.class ) );
  }

  @Test
  public void convert() throws Exception {
    TextToLongConverter c = new TextToLongConverter();
    Long expected = 100L;

    assertEquals( expected, c.convert( null, new Text( String.valueOf( expected ) ) ) );

    try {
      c.convert( null, null );
      fail();
    } catch ( NullPointerException ex ) {
      // Expected
    }

    try {
      c.convert( null, new Text( "not a long" ) );
    } catch ( TypeConversionException ex ) {
      assertTrue( ex.getMessage().contains( "!ErrorConverting!" ) );
    }

  }
}

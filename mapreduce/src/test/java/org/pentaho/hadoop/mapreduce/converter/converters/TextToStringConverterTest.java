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

import static org.junit.Assert.*;

public class TextToStringConverterTest {
  @Test
  public void canConvert() throws Exception {
    TextToStringConverter c = new TextToStringConverter();

    assertTrue( c.canConvert( Text.class, String.class ) );

    assertFalse( c.canConvert( null, null ) );
    assertFalse( c.canConvert( Text.class, Object.class ) );
    assertFalse( c.canConvert( Object.class, String.class ) );
  }

  @Test
  public void convert() throws Exception {
    TextToStringConverter c = new TextToStringConverter();
    String expected = "hello";

    assertEquals( expected, c.convert( null, new Text( expected ) ) );

    try {
      c.convert( null, null );
      fail();
    } catch ( NullPointerException ex ) {
      // Expected
    }
  }
}

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

import org.junit.Test;

import static org.junit.Assert.*;

public class NullConverterTest {
  @Test
  public void canConvert() throws Exception {
    NullConverter c = new NullConverter();

    assertTrue( c.canConvert( null, null ) );
    assertTrue( c.canConvert( null, Object.class ) );
    assertTrue( c.canConvert( Object.class, null ) );
    assertFalse( c.canConvert( String.class, Long.class ) );
    assertFalse( c.canConvert( Long.class, Object.class ) );
    assertFalse( c.canConvert( Object.class, Long.class ) );
  }

  @Test
  public void convert() throws Exception {
    NullConverter c = new NullConverter();

    assertNull( c.convert( null, new Object() ) );
    assertNull( c.convert( null, null ) );
  }
}

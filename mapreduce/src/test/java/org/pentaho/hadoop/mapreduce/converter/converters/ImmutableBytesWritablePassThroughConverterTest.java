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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ImmutableBytesWritablePassThroughConverterTest {

  @Test
  public void canConvert() throws Exception {
    ImmutableBytesWritablePassThroughConverter c =
      new ImmutableBytesWritablePassThroughConverter();

    assertTrue( c.canConvert( ImmutableBytesWritable.class, Object.class ) );
    assertFalse( c.canConvert( null, null ) );
    assertFalse( c.canConvert( Object.class, ImmutableBytesWritable.class ) );
  }
}

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

import org.apache.hadoop.hbase.client.Result;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ResultPassThroughConverterTest {

  @Test
  public void canConvert() throws Exception {
    ResultPassThroughConverter c = new ResultPassThroughConverter();

    assertTrue( c.canConvert( Result.class, Object.class ) );
    assertFalse( c.canConvert( null, null ) );
    assertFalse( c.canConvert( Object.class, Result.class ) );
  }
}

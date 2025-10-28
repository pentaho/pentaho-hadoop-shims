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


package org.pentaho.bigdata.api.mapreduce;

import org.junit.Test;
import org.pentaho.hadoop.shim.api.mapreduce.MapReduceExecutionException;

import static org.junit.Assert.assertEquals;

/**
 * Created by bryan on 12/8/15.
 */
public class MapReduceExecutionExceptionTest {
  @Test
  public void testStringConstructor() {
    String msg = "msg";
    assertEquals( msg, new MapReduceExecutionException( msg ).getMessage() );
  }

  @Test
  public void testThrowableConstructor() {
    Throwable throwable = new Throwable();
    assertEquals( throwable, new MapReduceExecutionException( throwable ).getCause() );
  }
}

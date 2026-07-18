/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/



package org.pentaho.hadoop.shim.api.hbase;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by bryan on 2/2/16.
 */
public class ResultFactoryExceptionTest {
  @Test
  public void testConstructor() {
    Exception exception = new Exception();
    assertEquals( exception, new ResultFactoryException( exception ).getCause() );
  }
}

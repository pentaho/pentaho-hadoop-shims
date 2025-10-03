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
import org.pentaho.hadoop.shim.api.mapreduce.TaskCompletionEvent;

import static org.junit.Assert.assertEquals;

/**
 * Created by bryan on 12/8/15.
 */
public class TaskCompletionEventStatusTest {
  @Test
  public void testEnum() {
    for ( TaskCompletionEvent.Status status : TaskCompletionEvent.Status.values() ) {
      assertEquals( status, TaskCompletionEvent.Status.valueOf( status.name() ) );
    }
  }
}

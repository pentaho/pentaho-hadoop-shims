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


package org.pentaho.big.data.impl.shim.mapreduce;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.hadoop.shim.api.internal.mapred.TaskCompletionEvent;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 12/8/15.
 */
public class TaskCompletionEventImplTest {
  private TaskCompletionEvent delegate;
  private TaskCompletionEventImpl taskCompletionEvent;

  @Before
  public void setup() {
    delegate = mock( TaskCompletionEvent.class );
    taskCompletionEvent = new TaskCompletionEventImpl( delegate );
  }

  @Test
  public void testGetTaskStatus() {
    for ( TaskCompletionEvent.Status status : TaskCompletionEvent.Status.values() ) {
      delegate = mock( TaskCompletionEvent.class );
      taskCompletionEvent = new TaskCompletionEventImpl( delegate );
      when( delegate.getTaskStatus() ).thenReturn( status );
      assertEquals( status.name(), taskCompletionEvent.getTaskStatus().name() );
    }
  }

  @Test
  public void testGetTaskAttemptId() {
    Object value = new Object();
    when( delegate.getTaskAttemptId() ).thenReturn( value );
    assertEquals( value, taskCompletionEvent.getTaskAttemptId() );
  }

  @Test
  public void testGetEventId() {
    int eventId = 30984;
    when( delegate.getEventId() ).thenReturn( eventId );
    assertEquals( eventId, taskCompletionEvent.getEventId() );
  }
}

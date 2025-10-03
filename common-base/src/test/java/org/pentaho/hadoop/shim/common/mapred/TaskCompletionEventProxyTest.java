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


package org.pentaho.hadoop.shim.common.mapred;

import static org.junit.Assert.*;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskID;
import org.junit.Test;
import org.pentaho.hadoop.shim.api.internal.mapred.TaskCompletionEvent;

@SuppressWarnings( "deprecation" )
public class TaskCompletionEventProxyTest {

  @Test( expected = NullPointerException.class )
  public void instantiation_null() {
    new TaskCompletionEventProxy( null );
  }

  @Test
  public void getTaskAttemptId() {
    final TaskAttemptID id = new TaskAttemptID( new TaskID(), 0 );
    org.apache.hadoop.mapred.TaskCompletionEvent delegate = new org.apache.hadoop.mapred.TaskCompletionEvent() {
      public org.apache.hadoop.mapred.TaskAttemptID getTaskAttemptId() {
        return id;
      }
    };
    TaskCompletionEventProxy proxy = new TaskCompletionEventProxy( delegate );

    assertEquals( id, proxy.getTaskAttemptId() );
  }

  @Test
  public void getTaskStatus() {
    final AtomicReference<org.apache.hadoop.mapred.TaskCompletionEvent.Status> status =
      new AtomicReference<org.apache.hadoop.mapred.TaskCompletionEvent.Status>();

    org.apache.hadoop.mapred.TaskCompletionEvent delegate = new org.apache.hadoop.mapred.TaskCompletionEvent() {
      public org.apache.hadoop.mapred.TaskCompletionEvent.Status getTaskStatus() {
        return status.get();
      }
    };
    TaskCompletionEventProxy proxy = new TaskCompletionEventProxy( delegate );

    status.set( org.apache.hadoop.mapred.TaskCompletionEvent.Status.FAILED );
    assertEquals( TaskCompletionEvent.Status.FAILED, proxy.getTaskStatus() );
    status.set( org.apache.hadoop.mapred.TaskCompletionEvent.Status.KILLED );
    assertEquals( TaskCompletionEvent.Status.KILLED, proxy.getTaskStatus() );
    status.set( org.apache.hadoop.mapred.TaskCompletionEvent.Status.OBSOLETE );
    assertEquals( TaskCompletionEvent.Status.OBSOLETE, proxy.getTaskStatus() );
    status.set( org.apache.hadoop.mapred.TaskCompletionEvent.Status.SUCCEEDED );
    assertEquals( TaskCompletionEvent.Status.SUCCEEDED, proxy.getTaskStatus() );
    status.set( org.apache.hadoop.mapred.TaskCompletionEvent.Status.TIPFAILED );
    assertEquals( TaskCompletionEvent.Status.TIPFAILED, proxy.getTaskStatus() );
  }

  @Test
  public void getEventId() {
    final int id = 12332;
    org.apache.hadoop.mapred.TaskCompletionEvent delegate = new org.apache.hadoop.mapred.TaskCompletionEvent() {
      public int getEventId() {
        return id;
      }
    };
    TaskCompletionEventProxy proxy = new TaskCompletionEventProxy( delegate );

    assertEquals( id, proxy.getEventId() );
  }

}

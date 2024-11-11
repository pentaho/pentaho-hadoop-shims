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

import org.pentaho.hadoop.shim.api.internal.mapred.TaskCompletionEvent;

public class TaskCompletionEventProxy implements TaskCompletionEvent {
  private org.apache.hadoop.mapred.TaskCompletionEvent delegate;

  public TaskCompletionEventProxy( org.apache.hadoop.mapred.TaskCompletionEvent delegateParam ) {
    if ( delegateParam == null ) {
      throw new NullPointerException();
    }
    this.delegate = delegateParam;
  }

  @Override
  public Object getTaskAttemptId() {
    return delegate.getTaskAttemptId();
  }

  @Override
  public Status getTaskStatus() {
    org.apache.hadoop.mapred.TaskCompletionEvent.Status s = delegate.getTaskStatus();
    switch ( s ) {
      case FAILED:
        return Status.FAILED;
      case KILLED:
        return Status.KILLED;
      case OBSOLETE:
        return Status.OBSOLETE;
      case SUCCEEDED:
        return Status.SUCCEEDED;
      case TIPFAILED:
        return Status.TIPFAILED;
      default:
        throw new IllegalStateException( "unknown status: " + s );
    }
  }

  @Override
  public int getEventId() {
    return delegate.getEventId();
  }
}

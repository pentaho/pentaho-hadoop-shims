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

package org.pentaho.big.data.impl.shim.mapreduce;


import org.pentaho.hadoop.shim.api.mapreduce.TaskCompletionEvent;

/**
 * Created by bryan on 12/3/15.
 */
public class TaskCompletionEventImpl implements TaskCompletionEvent {
  private final org.pentaho.hadoop.shim.api.internal.mapred.TaskCompletionEvent delegate;

  public TaskCompletionEventImpl( org.pentaho.hadoop.shim.api.internal.mapred.TaskCompletionEvent delegate ) {
    this.delegate = delegate;
  }

  @Override public Status getTaskStatus() {
    return Status.valueOf( delegate.getTaskStatus().toString() );
  }

  @Override public Object getTaskAttemptId() {
    return delegate.getTaskAttemptId();
  }

  @Override public int getEventId() {
    return delegate.getEventId();
  }
}

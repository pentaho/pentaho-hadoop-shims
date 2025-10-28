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

import java.io.IOException;

import org.apache.hadoop.mapred.TaskAttemptID;
import org.pentaho.hadoop.shim.api.internal.mapred.RunningJob;
import org.pentaho.hadoop.shim.api.internal.mapred.TaskCompletionEvent;

public class RunningJobProxy implements RunningJob {
  org.apache.hadoop.mapred.RunningJob delegate;

  public RunningJobProxy( org.apache.hadoop.mapred.RunningJob delegateParam ) {
    if ( delegateParam == null ) {
      throw new NullPointerException();
    }
    this.delegate = delegateParam;
  }

  @Override
  public boolean isComplete() throws IOException {
    return delegate.isComplete();
  }

  @Override
  public void killJob() throws IOException {
    delegate.killJob();
  }

  @Override
  public boolean isSuccessful() throws IOException {
    return delegate.isSuccessful();
  }

  @Override
  public TaskCompletionEvent[] getTaskCompletionEvents( int startIndex ) throws IOException {
    org.apache.hadoop.mapred.TaskCompletionEvent[] events = delegate.getTaskCompletionEvents( startIndex );
    TaskCompletionEvent[] wrapped = new TaskCompletionEvent[ events.length ];

    for ( int i = 0; i < wrapped.length; i++ ) {
      wrapped[ i ] = new TaskCompletionEventProxy( events[ i ] );
    }

    return wrapped;
  }

  @Override
  public String[] getTaskDiagnostics( Object taskAttemptId ) throws IOException {
    @SuppressWarnings( "deprecation" )
    TaskAttemptID id = (TaskAttemptID) taskAttemptId;
    return delegate.getTaskDiagnostics( id );
  }

  @Override
  public float setupProgress() throws IOException {
    return delegate.setupProgress();
  }

  @Override
  public float mapProgress() throws IOException {
    return delegate.mapProgress();
  }

  @Override
  public float reduceProgress() throws IOException {
    return delegate.reduceProgress();
  }
}

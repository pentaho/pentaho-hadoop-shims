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


import org.pentaho.hadoop.shim.api.internal.mapred.RunningJob;
import org.pentaho.hadoop.shim.api.mapreduce.MapReduceJobAdvanced;
import org.pentaho.hadoop.shim.api.mapreduce.MapReduceService;
import org.pentaho.hadoop.shim.api.mapreduce.TaskCompletionEvent;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by bryan on 12/3/15.
 */
public class RunningJobMapReduceJobAdvancedImpl implements MapReduceJobAdvanced {
  private final RunningJob runningJob;

  public RunningJobMapReduceJobAdvancedImpl( RunningJob runningJob ) {
    this.runningJob = runningJob;
  }

  @Override public void killJob() throws IOException {
    runningJob.killJob();
  }

  @Override public boolean waitOnCompletion( long timeout, TimeUnit timeUnit, MapReduceService.Stoppable stoppable )
    throws IOException, InterruptedException {
    long stopTime = System.currentTimeMillis() + timeUnit.toMillis( timeout );
    long sleepTime;
    while ( !stoppable.isStopped() && ( sleepTime = Math.min( 50, stopTime - System.currentTimeMillis() ) ) > 0
      && !runningJob.isComplete() ) {
      Thread.sleep( Math.max( 0, sleepTime ) );
    }
    return runningJob.isComplete();
  }

  @Override public double getSetupProgress() throws IOException {
    return runningJob.setupProgress();
  }

  @Override public double getMapProgress() throws IOException {
    return runningJob.mapProgress();
  }

  @Override public double getReduceProgress() throws IOException {
    return runningJob.reduceProgress();
  }

  @Override public boolean isSuccessful() throws IOException {
    return runningJob.isSuccessful();
  }

  @Override public boolean isComplete() throws IOException {
    return runningJob.isComplete();
  }

  @Override public TaskCompletionEvent[] getTaskCompletionEvents( int startIndex ) throws IOException {
    final org.pentaho.hadoop.shim.api.internal.mapred.TaskCompletionEvent[] taskCompletionEvents =
      runningJob.getTaskCompletionEvents( startIndex );

    TaskCompletionEvent[] result = new TaskCompletionEvent[ taskCompletionEvents.length ];
    for ( int i = 0; i < taskCompletionEvents.length; i++ ) {
      result[ i ] = new TaskCompletionEventImpl( taskCompletionEvents[ i ] );
    }
    return result;
  }

  @Override public String[] getTaskDiagnostics( Object taskAttemptId ) throws IOException {
    return runningJob.getTaskDiagnostics( taskAttemptId );
  }
}

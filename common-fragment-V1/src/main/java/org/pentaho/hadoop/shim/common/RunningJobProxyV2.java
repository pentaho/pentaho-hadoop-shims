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

package org.pentaho.hadoop.shim.common;

import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.Job;
import org.pentaho.hadoop.shim.api.internal.mapred.RunningJob;
import org.pentaho.hadoop.shim.common.mapred.TaskCompletionEventProxy;

import java.io.IOException;

/**
 * User: Dzmitry Stsiapanau Date: 7/22/14 Time: 3:50 PM
 */
public class RunningJobProxyV2 implements RunningJob {
  private Job delegateJob;

  public RunningJobProxyV2( Job job ) {
    delegateJob = job;
  }


  /**
   * Check if the job is completed.
   *
   * @return {@code true} if the job has completed
   * @throws java.io.IOException
   */
  @Override
  public boolean isComplete() throws IOException {
    return delegateJob.isComplete();
  }

  /**
   * Kill a running job. This blocks until all tasks of the job have been killed as well.
   *
   * @throws java.io.IOException
   */
  @Override
  public void killJob() throws IOException {
    delegateJob.killJob();
  }

  /**
   * Check if a job completed successfully.
   *
   * @return {@code true} if the job succeeded.
   * @throws java.io.IOException
   */
  @Override
  public boolean isSuccessful() throws IOException {
    return delegateJob.isSuccessful();
  }

  /**
   * Get a list of events indicating success/failure of underlying tasks.
   *
   * @param startIndex offset/index to start fetching events from
   * @return an array of events
   * @throws java.io.IOException
   */
  @Override public org.pentaho.hadoop.shim.api.internal.mapred.TaskCompletionEvent[] getTaskCompletionEvents(
    int startIndex )
    throws IOException {
    org.apache.hadoop.mapred.TaskCompletionEvent[] events = delegateJob.getTaskCompletionEvents( startIndex );
    org.pentaho.hadoop.shim.api.internal.mapred.TaskCompletionEvent[] wrapped =
      new org.pentaho.hadoop.shim.api.internal.mapred.TaskCompletionEvent[ events.length ];

    for ( int i = 0; i < wrapped.length; i++ ) {
      wrapped[ i ] = new TaskCompletionEventProxy( events[ i ] );
    }

    return wrapped;
  }

  /**
   * Retrieve the diagnostic messages for a given task attempt.
   *
   * @param taskAttemptId Identifier of the task
   * @return an array of diagnostic messages for the task attempt with the id provided.
   * @throws java.io.IOException
   */
  @Override public String[] getTaskDiagnostics( Object taskAttemptId ) throws IOException {
    TaskAttemptID id = (TaskAttemptID) taskAttemptId;
    try {
      return delegateJob.getTaskDiagnostics( id );
    } catch ( InterruptedException e ) {
      throw new RuntimeException( e );
    }
  }

  /**
   * The progress of the job's setup tasks.
   *
   * @return progress percentage
   * @throws java.io.IOException
   */
  @Override public float setupProgress() throws IOException {
    return delegateJob.setupProgress();
  }

  /**
   * The progress of the job's map tasks.
   *
   * @return progress percentage
   * @throws java.io.IOException
   */
  @Override public float mapProgress() throws IOException {
    return delegateJob.mapProgress();
  }

  /**
   * The progress of the job's reduce tasks.
   *
   * @return progress percentage
   * @throws java.io.IOException
   */
  @Override public float reduceProgress() throws IOException {
    return delegateJob.reduceProgress();
  }
}

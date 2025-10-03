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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskCompletionEvent;

public class MockRunningJob implements RunningJob {

  @Override
  public float cleanupProgress() throws IOException {
    return 0;
  }

  @Override
  public Counters getCounters() throws IOException {
    return null;
  }

  @Override
  public JobID getID() {
    return null;
  }

  @Override
  public String getJobFile() {
    return null;
  }

  @Override
  @Deprecated
  public String getJobID() {
    return null;
  }

  @Override
  public String getJobName() {
    return null;
  }

  @Override
  public int getJobState() throws IOException {
    return 0;
  }

  @Override
  public TaskCompletionEvent[] getTaskCompletionEvents( int arg0 ) throws IOException {
    return null;
  }

  @Override
  public String[] getTaskDiagnostics( TaskAttemptID arg0 ) throws IOException {
    return null;
  }

  @Override
  public String getTrackingURL() {
    return null;
  }

  @Override
  public boolean isComplete() throws IOException {
    return false;
  }

  @Override
  public boolean isSuccessful() throws IOException {
    return false;
  }

  @Override
  public void killJob() throws IOException {
  }

  @Override
  public void killTask( TaskAttemptID arg0, boolean arg1 ) throws IOException {
  }

  @Override
  @Deprecated
  public void killTask( String arg0, boolean arg1 ) throws IOException {
  }

  @Override
  public float mapProgress() throws IOException {
    return 0;
  }

  @Override
  public float reduceProgress() throws IOException {
    return 0;
  }

  @Override
  public void setJobPriority( String arg0 ) throws IOException {
  }

  @Override
  public float setupProgress() throws IOException {
    return 0;
  }

  @Override
  public void waitForCompletion() throws IOException {
  }

  // Omit @Override since not all Hadoop versions define this method 
  public String getFailureInfo() throws IOException {
    return null;
  }

  //Omit @Override since not all Hadoop versions define this method 
  public JobStatus getJobStatus() throws IOException {
    return null;
  }

  //Omit @Override since not all Hadoop versions define this method
  public int unBlackListTracker( String arg0 ) {
    return 0;
  }

  public org.apache.hadoop.mapred.TaskCompletionEventList getTaskCompletionEventList( int arg0 ) {
    return null;
  }

  public boolean isRetired() throws java.io.IOException {
    return false;
  }

  public Configuration getConfiguration() {
    return null;
  }

  public String getHistoryUrl() throws IOException {
    return null;
  }
}

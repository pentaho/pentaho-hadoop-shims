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

package org.pentaho.hadoop.shim.api.mapreduce;

import java.io.IOException;

/**
 * MapReduce job interface that supports progress monitoring, completion events, and task diagnostics
 */
public interface MapReduceJobAdvanced extends MapReduceJob {
  /**
   * Returns the setup progress
   *
   * @return the setup progress
   * @throws IOException
   */
  double getSetupProgress() throws IOException;

  /**
   * Returns the map progress
   *
   * @return the map progress
   * @throws IOException
   */
  double getMapProgress() throws IOException;

  /**
   * Returns the reduce progress
   *
   * @return the reduce progress
   * @throws IOException
   */
  double getReduceProgress() throws IOException;

  /**
   * Returns the TaskCompletionEvents starting at the given start index
   *
   * @param startIndex the start index
   * @return the TaskCompletionEvents starting at the given start index
   * @throws IOException
   */
  TaskCompletionEvent[] getTaskCompletionEvents( int startIndex ) throws IOException;

  /**
   * Returns the task diagnostics for a given attempt id
   *
   * @param taskAttemptId the attempt id
   * @return the task diagnostics for a given attempt id
   * @throws IOException
   */
  String[] getTaskDiagnostics( Object taskAttemptId ) throws IOException;
}

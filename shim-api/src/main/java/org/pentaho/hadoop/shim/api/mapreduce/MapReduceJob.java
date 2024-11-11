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
import java.util.concurrent.TimeUnit;

/**
 * Common interface for a running MapReduce job.
 */
public interface MapReduceJob {
  /**
   * Kills the job
   *
   * @throws IOException
   */
  void killJob() throws IOException;

  /**
   * Wait for up to timeout time units (ex: 10 seconds) for the job to complete.
   *
   * @param timeout   timeout value
   * @param timeUnit  the time units of the timeout value
   * @param stoppable boolean callback taht can terminate the wait early (optional in implementations)
   * @return a boolean indicating whether the job completed during the wait
   * @throws IOException
   * @throws InterruptedException
   * @throws MapReduceExecutionException
   */
  boolean waitOnCompletion( long timeout, TimeUnit timeUnit, MapReduceService.Stoppable stoppable )
    throws IOException, InterruptedException, MapReduceExecutionException;

  /**
   * Returns a boolean indicating whether the MapReduce job was successful
   *
   * @return a boolean indicating whether the MapReduce job was successful
   * @throws IOException
   */
  boolean isSuccessful() throws IOException;

  /**
   * Returns a boolean indicating whether the MapReduce job is complete
   *
   * @return a boolean indicating whether the MapReduce job is complete
   * @throws IOException
   */
  boolean isComplete() throws IOException;
}

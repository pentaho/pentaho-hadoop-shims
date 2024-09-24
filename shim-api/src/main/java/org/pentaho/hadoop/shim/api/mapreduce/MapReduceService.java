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

import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;

import java.io.IOException;
import java.net.URL;

/**
 * Interface for creating MapReduce jobs
 */
public interface MapReduceService {
  /**
   * Executes the main method in a jar that is responsible to submitting a MapReduce job
   *
   * @param resolvedJarUrl  the jar url
   * @param driverClass     the main class
   * @param commandLineArgs command line arguments
   * @return a MapReduceJobSimple reference for tracking progress
   * @throws MapReduceExecutionException
   */
  MapReduceJobSimple executeSimple( URL resolvedJarUrl, String driverClass, String commandLineArgs )
    throws MapReduceExecutionException;

  /**
   * Returns a MapReduceJobBuilder for configuring and launching a more complex MapReduce job
   *
   * @param log           the log
   * @param variableSpace the variable space
   * @return a MapReduceJobBuilder
   */
  MapReduceJobBuilder createJobBuilder( LogChannelInterface log, VariableSpace variableSpace );

  PentahoMapReduceJobBuilder createPentahoMapReduceJobBuilder( LogChannelInterface log, VariableSpace variableSpace )
    throws IOException;

  /**
   * Returns relevant information on a jar
   *
   * @param resolvedJarUrl the jar url
   * @return relevant information on a jar
   * @throws IOException
   * @throws ClassNotFoundException
   */
  MapReduceJarInfo getJarInfo( URL resolvedJarUrl ) throws IOException, ClassNotFoundException;

  /**
   * Interface for clients to implement if they would like to terminate the wait functionality before the timeout
   */
  interface Stoppable {
    /**
     * Returns a boolean indicating whether the parent process is stopped
     *
     * @return a boolean indicating whether the parent process is stopped
     */
    boolean isStopped();
  }
}

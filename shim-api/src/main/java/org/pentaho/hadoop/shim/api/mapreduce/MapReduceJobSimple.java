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

/**
 * MapReduce job from running jar file
 */
public interface MapReduceJobSimple extends MapReduceJob {

  /**
   * Returns the main class
   *
   * @return the main class
   */
  String getMainClass();

  /**
   * Returns the exit code
   *
   * @return the exit code
   */
  int getStatus();
}

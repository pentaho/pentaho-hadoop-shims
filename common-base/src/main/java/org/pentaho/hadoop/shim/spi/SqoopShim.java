/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2002 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/


package org.pentaho.hadoop.shim.spi;

import org.pentaho.hadoop.shim.api.internal.Configuration;

/**
 * Provides a simple abstraction for executing a Sqoop tool.
 *
 * @author Jordan Ganoff (jganoff@pentaho.com)
 */
public interface SqoopShim extends PentahoHadoopShim {
  /**
   * Execute Sqoop with the provided arguments and configuration. This entry-point parses the correct SqoopTool to use
   * from the args.
   *
   * @see org.apache.sqoop.Sqoop#runTool(String[], org.apache.hadoop.conf.Configuration)
   */
  int runTool( String[] args, Configuration c );
}
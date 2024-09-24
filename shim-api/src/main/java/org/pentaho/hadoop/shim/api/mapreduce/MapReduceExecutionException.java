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
 * Created by bryan on 12/2/15.
 */
public class MapReduceExecutionException extends Exception {
  public MapReduceExecutionException( Throwable cause ) {
    super( cause );
  }

  public MapReduceExecutionException( String message ) {
    super( message );
  }
}

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
package org.pentaho.hadoop.shim.common.format;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class ReadFileFilter implements PathFilter, Configurable {
  public static final String FILTER_DIR = "PentahoParquetFilterDir";
  public static final String FILTER_FILE = "PentahoParquetFilterFile";

  private Configuration conf;

  @Override
  public void setConf( Configuration conf ) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public boolean accept( Path path ) {
    String requiredDir = conf.get( FILTER_DIR );
    if ( requiredDir == null ) {
      throw new RuntimeException( "Required dir not defined" );
    }
    String requiredFile = conf.get( FILTER_FILE );
    if ( requiredFile == null ) {
      throw new RuntimeException( "Required file not defined" );
    }
    return path.equals( new Path( requiredDir ) ) || path.equals( new Path( requiredFile ) );
  }
}

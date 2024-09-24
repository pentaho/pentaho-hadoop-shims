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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;

public class ReadFilesFilter implements PathFilter, Configurable {
  public static final String FILTER_DIR = "PentahoParquetFilterDir";
  public static final String FILTER_FILE = "PentahoParquetFilterFile";
  public static final String DIRECTORY = "PentahoParquetDir";
  public static final String FILE = "PentahoParquetFile";

  private Configuration conf;

  @Override public Configuration getConf() {
    return conf;
  }

  @Override public void setConf( Configuration conf ) {
    this.conf = conf;
  }

  @SuppressWarnings( "squid:S112" )
  @Override public boolean accept( Path path ) {
    boolean returnValue = false;
    try {
      FileSystem fs = path.getFileSystem( conf );
      if ( ( conf.get( DIRECTORY ) != null && fs.isDirectory( path ) ) || ( conf.get( FILE ) != null && fs
        .isFile( path ) ) ) {
        returnValue = true;
      }

      if ( conf.get( DIRECTORY ) == null && conf.get( FILE ) == null ) {
        throw new RuntimeException( "Required configuration is  not defined" );
      }
      return returnValue;
    } catch ( IOException ex ) {
      return returnValue;
    }
  }
}

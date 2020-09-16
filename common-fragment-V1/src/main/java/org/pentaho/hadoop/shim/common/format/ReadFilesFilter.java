/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2019-2020 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
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

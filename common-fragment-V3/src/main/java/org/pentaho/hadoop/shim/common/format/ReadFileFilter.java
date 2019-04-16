/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2017 by Hitachi Vantara : http://www.pentaho.com
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

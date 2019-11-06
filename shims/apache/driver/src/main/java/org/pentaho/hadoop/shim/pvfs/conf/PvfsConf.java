/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.hadoop.shim.pvfs.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.pentaho.di.connections.ConnectionDetails;


public abstract class PvfsConf {

  final ConnectionDetails details;

  PvfsConf( ConnectionDetails details ) {
    this.details = details;
  }

  public abstract boolean supportsConnection();

  public abstract Path mapPath( Path pvfsPath );

  public abstract Configuration conf( Path pvfsPath );

  void validatePath( Path pvfsPath ) {
    if ( !supportsConnection() || !pvfsPath.toUri().getScheme().equals( "pvfs" ) ) {
      throw new IllegalStateException( pvfsPath.toString() + " not supported by " + details.getClass().getName() );
    }
  }

  @FunctionalInterface
  public interface ConfFactory {
    PvfsConf get( ConnectionDetails details );
  }
}

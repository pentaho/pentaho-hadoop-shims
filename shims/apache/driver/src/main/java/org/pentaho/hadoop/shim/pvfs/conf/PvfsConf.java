/*******************************************************************************
 *
 * Pentaho Big Data
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

package org.pentaho.hadoop.shim.pvfs.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.pentaho.di.connections.ConnectionDetails;

import java.util.Objects;

/**
 * Base class for filesystem configuration classes.  Implementations
 * will handle population of hadoop Configuration objects based on
 * named vfs ConnectionDetails, as well as mapping paths to/from
 * pvfs:// schemes.
 */
public abstract class PvfsConf {

  protected final ConnectionDetails details;

  PvfsConf( ConnectionDetails details ) {
    this.details = details;
  }

  /**
   * Tests whether this PvfsConf can be used for the connection
   * details provided.  HCP connection details, for example, cannot
   * be used with S3Conf.
   */
  public abstract boolean supportsConnection();

  /**
   * Maps a given path with scheme 'pvfs:' to the appropriate
   * "real" filesystem path by looking up the connection details from
   * the named vfs connection.
   */
  public abstract Path mapPath( Path pvfsPath );

  /**
   * Maps a "real" filesystem path to the pvfs:// form.
   * For example, given pvfsPath 'pvfs://s3Conn/path' and
   * realFsPath 's3://bucket/somedir/somefile.txt', this
   * method will determine the correct pvfs path for 'somefile.txt':
   *   'pvfs://s3Conn/bucket/somedir/somefile.txt'
   *
   * Different PvfsConf implementations will have different rules
   * for mapping a realFsPath back to a pvfsPath.
   */
  public abstract Path mapPath( Path pvfsPath, Path realFsPath );

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

  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }

    PvfsConf pvfsConf = (PvfsConf) o;
    if ( details == null && pvfsConf.details == null ) {
      return true;
    }
    if ( details == null || pvfsConf.details == null ) {
      // one but not the other is null
      return false;
    }
    return Objects.equals( details.getProperties(), pvfsConf.details.getProperties() );
  }

  @Override public int hashCode() {
    if ( details == null ) {
      return Objects.hash( this );
    }
    return Objects.hash( details.getProperties() );
  }

  @SuppressWarnings( "squid:S1172" )
  public String generateAlias( String pvfsPath ) {
    return null;
  }
}

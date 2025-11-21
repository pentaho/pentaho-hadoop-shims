/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.hadoop.shim.common.pvfs.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.pentaho.di.connections.ConnectionDetails;

import java.util.Map;
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

  protected String getVar( String value ) {
    if ( details.getSpace() != null ) {
      return details.getSpace().environmentSubstitute( value );
    }
    return value;
  }

  protected String getVar( Map<String, String> properties, String fieldName ) {
    return getVar( properties.get( fieldName ) );
  }
}

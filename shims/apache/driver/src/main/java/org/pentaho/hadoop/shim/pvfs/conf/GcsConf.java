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


package org.pentaho.hadoop.shim.pvfs.conf;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.pentaho.di.connections.ConnectionDetails;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.hadoop.fs.Path.SEPARATOR;
import static org.pentaho.hadoop.shim.pvfs.PvfsHadoopBridge.getConnectionName;

public class GcsConf extends PvfsConf {

  private final String credentialsKeyPath;

  public GcsConf( ConnectionDetails details ) {
    super( details );
    credentialsKeyPath = details.getProperties().get( "keyPath" );
  }

  @Override public boolean supportsConnection() {
    return GoogleCloudStorageFileSystem.SCHEME.equalsIgnoreCase( details.getType() );
  }

  @Override public Path mapPath( Path pvfsPath ) {
    validatePath( pvfsPath );
    String[] splitPath = pvfsPath.toUri().getPath().split( "/" );

    Preconditions.checkArgument( splitPath.length > 0 );
    String bucket = splitPath[1];
    String path = SEPARATOR + Arrays.stream( splitPath ).skip( 2 ).collect( Collectors.joining( SEPARATOR ) );
    try {
      return new Path( new URI( GoogleCloudStorageFileSystem.SCHEME, bucket, path, null ) );
    } catch ( URISyntaxException e ) {
      throw new IllegalStateException( e );
    }
  }

  @Override public Path mapPath( Path pvfsPath, Path realFsPath ) {
    URI uri = realFsPath.toUri();
    return new Path( pvfsPath.toUri().getScheme(),
            getConnectionName( pvfsPath ), "/" + uri.getHost() + uri.getPath() );
  }

  @Override public Configuration conf( Path pvfsPath ) {
    /**
     * GCS Connector configurations can be found here :
     * https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/conf/gcs-core-default.xml
     */
    Configuration config = new Configuration();
    config.set( "fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem" );
    config.set( "fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS" );
    config.set( "google.cloud.auth.service.account.enable", "true" );
    config.set( "google.cloud.auth.service.account.json.keyfile", credentialsKeyPath );
    config.set( "fs.gs.http.max.retry", "10" );
    config.set( "fs.gs.http.connect-timeout", "20000" );
    config.set( "fs.gs.performance.cache.enable", "false" ); // caching managed by PvfsHadoopBridge
    return config;
  }

  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    if ( !super.equals( o ) ) {
      return false;
    }
    GcsConf gcsConf = (GcsConf) o;
    return Objects.equals( credentialsKeyPath, gcsConf.credentialsKeyPath );
  }

  @Override public int hashCode() {
    return Objects.hash( super.hashCode(), credentialsKeyPath );
  }
}

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

package org.pentaho.hadoop.shim.common.format;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.pentaho.di.connections.ConnectionDetails;
import org.pentaho.di.connections.ConnectionManager;
import org.pentaho.di.core.encryption.Encr;

import java.net.URI;
import java.util.Optional;
import java.util.function.Supplier;

public class S3NCredentialUtils {

  private static final String S3NSCHEME = "s3n";
  private static final String S3ASCHEME = "s3a";
  private static final String S3NROOTBUCKET = S3NSCHEME + "/";
  private static final String DEFAULT_S3_CONFIG_PROPERTY = "defaultS3Config";
  private static boolean s3nIsSupported = true;
  private final Supplier<ConnectionManager> connectionManager = ConnectionManager::getInstance;

  public static String scrubFilePathIfNecessary( String filename ) {
    if ( filename != null ) {
      filename = filename.replace( S3NROOTBUCKET, "" );
      if ( !s3nIsSupported ) {
        filename = filename.replace( S3NSCHEME, S3ASCHEME );
      }
    }
    return filename;
  }

  public static boolean isS3nIsSupported() {
    return s3nIsSupported;
  }

  public static void setS3nIsSupported( boolean supported ) {
    s3nIsSupported = supported;
  }

  public void applyS3CredentialsToHadoopConfigurationIfNecessary( String filename, Configuration conf ) {
    Path outputFile = new Path( scrubFilePathIfNecessary( filename ) );
    URI uri = outputFile.toUri();
    String scheme = uri != null ? uri.getScheme() : null;

    Optional<? extends ConnectionDetails> s3Connection = Optional.empty();
    try {
      s3Connection =
        connectionManager.get().getConnectionDetailsByScheme( "s3" ).stream().filter(
          connectionDetails -> connectionDetails.getProperties().get( DEFAULT_S3_CONFIG_PROPERTY ) != null
            && connectionDetails.getProperties().get( DEFAULT_S3_CONFIG_PROPERTY ).equalsIgnoreCase( "true" ) )
          .findFirst();
    } catch ( Exception ignored ) {
      // Ignore the exception. It's ok if we don't have a default S3 VFS connection.
    }

    if ( scheme == null || !s3Connection.isPresent() ) {
      return;
    }

    String accessKeyId = Encr.decryptPasswordOptionallyEncrypted( s3Connection.get().getProperties().get( "accessKey" ) );
    String secretKey = Encr.decryptPasswordOptionallyEncrypted( s3Connection.get().getProperties().get( "secretKey" ) );
    String endpointUrl = s3Connection.get().getProperties().get( "endpoint" );
    String pathStyleAccess = s3Connection.get().getProperties().get( "pathStyleAccess" );
    if ( scheme.equals( S3NSCHEME ) ) {
      conf.set( "fs.s3n.awsAccessKeyId", accessKeyId );
      conf.set( "fs.s3n.awsSecretAccessKey", secretKey );
      conf.set( "fs.s3.buffer.dir", System.getProperty( "java.io.tmpdir" ) );
      if ( endpointUrl != null ) {
        conf.set( "fs.s3n.endpoint", endpointUrl );
      }
    }

    if ( scheme.equals( S3ASCHEME ) ) {
      if ( pathStyleAccess == null ) {
        conf.set( "fs.s3a.path.style.access", "true" );
      } else {
        conf.set( "fs.s3a.path.style.access", pathStyleAccess );
      }

      conf.set( "fs.s3a.access.key", accessKeyId );
      conf.set( "fs.s3a.secret.key", secretKey );
      if ( endpointUrl != null ) {
        conf.set( "fs.s3a.endpoint", endpointUrl );
      }
    }
  }
}

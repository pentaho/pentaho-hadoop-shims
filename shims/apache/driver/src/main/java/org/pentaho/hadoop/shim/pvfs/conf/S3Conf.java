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

package org.pentaho.hadoop.shim.pvfs.conf;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.pentaho.di.connections.ConnectionDetails;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.google.common.base.Strings.isNullOrEmpty;
import static org.apache.hadoop.fs.Path.SEPARATOR;
import static org.pentaho.hadoop.shim.pvfs.PvfsHadoopBridge.getConnectionName;


public class S3Conf extends PvfsConf {

  private final String accessKey;
  private final String secretKey;
  private final String sessionToken;
  private String endpoint;
  private String pathStyleAccess;
  private final String credentialsFilePath;

  public S3Conf( ConnectionDetails details ) {
    super( details );
    Map<String, String> props = details.getProperties();
    credentialsFilePath = getVar( props, "credentialsFilePath" );

    if ( shouldGetCredsFromFile( getVar( props, "accessKey" ), getVar( props, "credentialsFilePath" ) ) ) {
      AWSCredentials creds = getCredsFromFile( props, credentialsFilePath );
      accessKey = creds.getAWSAccessKeyId();
      secretKey = creds.getAWSSecretKey();
      if ( creds instanceof BasicSessionCredentials ) {
        sessionToken = ( (BasicSessionCredentials) creds ).getSessionToken();
      } else {
        sessionToken = null;
      }
    } else {
      accessKey = getVar( props, "accessKey" );
      secretKey = getVar( props, "secretKey" );
      sessionToken = getVar( props, "sessionToken" );
      // Use only when VFS is configured for generic S3 connection
      endpoint = getVar( props, "endpoint" );
      pathStyleAccess = getVar( props, "pathStyleAccess" );
    }
  }

  @Override public boolean supportsConnection() {
    return Arrays.asList( "s3", "s3a", "s3n" ).contains( details.getType() );
  }

  @Override public Path mapPath( Path pvfsPath ) {
    validatePath( pvfsPath );
    String[] splitPath = pvfsPath.toUri().getPath().split( "/" );

    Preconditions.checkArgument( splitPath.length > 0 );
    String bucket = splitPath[ 1 ];
    String path = SEPARATOR + Arrays.stream( splitPath ).skip( 2 ).collect( Collectors.joining( SEPARATOR ) );
    try {
      return new Path( new URI( "s3a", bucket, path, null ) );
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
    validatePath( pvfsPath );
    Configuration conf = new Configuration();
    conf.set( "fs.s3a.access.key", accessKey );
    conf.set( "fs.s3a.secret.key", secretKey );
    if ( !isNullOrEmpty( sessionToken ) ) {
      // use of session token requires the TemporaryAWSCredentialProvider
      conf.set( "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider" );
      conf.set( "fs.s3a.session.token", sessionToken );
    }
    conf.set( "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem" );
    conf.set( "fs.s3a.connection.ssl.enabled", "true" );
    conf.set( "fs.s3a.attempts.maximum", "3" );

    conf.set( "fs.s3a.impl.disable.cache", "true" ); // caching managed by PvfsHadoopBridge

    conf.set( "fs.s3a.buffer.dir", System.getProperty( "java.io.tmpdir" ) );

    // Use only when VFS is configured for generic S3 connection
    if ( !isNullOrEmpty( endpoint ) ) {
      conf.set( "fs.s3a.endpoint", endpoint );
    }

    if ( !isNullOrEmpty( pathStyleAccess ) ) {
      conf.set( "fs.s3a.path.style.access", pathStyleAccess );
    }

    return conf;
  }

  private boolean shouldGetCredsFromFile( String accessKey, String credentialsFilePath ) {
    return isNullOrEmpty( accessKey ) && !isNullOrEmpty( credentialsFilePath );
  }

  private AWSCredentials getCredsFromFile( Map<String, String> props, String credentialsFilePath ) {
    try {
      ProfileCredentialsProvider credProvider =
        new ProfileCredentialsProvider( credentialsFilePath, props.get( "profileName" ) );
      return credProvider.getCredentials();
    } catch ( Exception e ) {
      throw new IllegalArgumentException(
        String.format( "Failed to load credentials for profile [%s] from %s",
          props.get( "profileName" ), credentialsFilePath ), e );
    }
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
    S3Conf s3Conf = (S3Conf) o;
    return Objects.equals( accessKey, s3Conf.accessKey )
      && Objects.equals( secretKey, s3Conf.secretKey )
      && Objects.equals( sessionToken, s3Conf.sessionToken )
      && Objects.equals( endpoint, s3Conf.endpoint )
      && Objects.equals( pathStyleAccess, s3Conf.pathStyleAccess )
      && Objects.equals( credentialsFilePath, s3Conf.credentialsFilePath );
  }

  @Override public int hashCode() {
    return Objects.hash( super.hashCode(), accessKey, secretKey, sessionToken, endpoint, pathStyleAccess,
      credentialsFilePath );
  }
}

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
    credentialsFilePath = props.get( "credentialsFilePath" );

    if ( shouldGetCredsFromFile( props.get( "accessKey" ), props.get( "credentialsFilePath" ) ) ) {
      AWSCredentials creds = getCredsFromFile( props, credentialsFilePath );
      accessKey = creds.getAWSAccessKeyId();
      secretKey = creds.getAWSSecretKey();
      if ( creds instanceof BasicSessionCredentials ) {
        sessionToken = ( (BasicSessionCredentials) creds ).getSessionToken();
      } else {
        sessionToken = null;
      }
    } else {
      accessKey = props.get( "accessKey" );
      secretKey = props.get( "secretKey" );
      sessionToken = props.get( "sessionToken" );
      // Use only when VFS is configured for generic S3 connection
      endpoint = props.get( "endpoint" );
      pathStyleAccess = props.get( "pathStyleAccess" );
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
    conf.set( "fs.s3a.endpoint", endpoint );
    conf.set( "fs.s3a.path.style.access", pathStyleAccess );

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
    return Objects.hash( super.hashCode(), accessKey, secretKey, sessionToken, endpoint, pathStyleAccess, credentialsFilePath );
  }
}

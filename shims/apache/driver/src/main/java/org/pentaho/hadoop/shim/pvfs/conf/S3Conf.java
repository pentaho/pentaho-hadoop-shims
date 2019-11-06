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
import java.util.stream.Collectors;

import static com.google.common.base.Strings.isNullOrEmpty;
import static org.apache.hadoop.fs.Path.SEPARATOR;


public class S3Conf extends PvfsConf {

  public S3Conf( ConnectionDetails details ) {
    super( details );
  }

  @Override public boolean supportsConnection() {
    return Arrays.asList( "s3", "s3a", "s3n" ).contains( details.getType() );
  }

  @Override public Path mapPath( Path pvfsPath ) {
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

  @Override public Configuration conf( Path pvfsPath ) {
    Configuration conf = new Configuration();
    Map<String, String> props = details.getProperties();

    String accessKey = props.get( "accessKey" );
    String secretKey = props.get( "secretKey" );
    String sessionToken = props.get( "sessionToken" );
    String credentialsFilePath = props.get( "credentialsFilePath" );
    if ( isNullOrEmpty( accessKey ) && !isNullOrEmpty( credentialsFilePath ) ) {
      AWSCredentials creds = getCredsFromFile( props, credentialsFilePath );
      accessKey = creds.getAWSAccessKeyId();
      secretKey = creds.getAWSSecretKey();

      if ( creds instanceof BasicSessionCredentials ) {
        sessionToken = ( (BasicSessionCredentials) creds ).getSessionToken();
      }
    }
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

    conf.set( "fs.s3.buffer.dir", System.getProperty( "java.io.tmpdir" ) );
    return conf;
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
}

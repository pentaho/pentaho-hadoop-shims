/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2020 by Hitachi Vantara : http://www.pentaho.com
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

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.adl.AdlConfKeys;
import org.apache.hadoop.fs.adl.AdlFileSystem;
import org.pentaho.di.connections.ConnectionDetails;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.google.common.base.Strings.isNullOrEmpty;
import static org.apache.hadoop.fs.Path.SEPARATOR;
import static org.pentaho.hadoop.shim.pvfs.PvfsHadoopBridge.getConnectionName;

public class ADLSGen1Conf extends PvfsConf {

  private static final String AZURE_AUTH_TYPE = "fs.adl.oauth2.access.token.provider.type";

  private final String accountFQDN;
  private final String scheme;
  private String clientId;
  private String clientSecret;
  private String authTokenEndpoint;

  public ADLSGen1Conf(ConnectionDetails details ) {
    super( details );
    try ( AdlFileSystem adlFileSystem = new AdlFileSystem() ) {
      scheme = adlFileSystem.getScheme();
      accountFQDN = details.getProperties().get( "accountFQDN" );
      if ( isServiceToServiceAuthentication( details.getProperties().get( "clientId" ),
        details.getProperties().get( "clientSecret" ), details.getProperties().get( "authTokenEndpoint" ) ) ) {
        clientId = details.getProperties().get( "clientId" );
        clientSecret = details.getProperties().get( "clientSecret" );
        authTokenEndpoint = details.getProperties().get( "authTokenEndpoint" );
      }
    } catch ( IOException e ) {
      throw new IllegalStateException( e );
    }
  }

  @Override
  public boolean supportsConnection() {
    return scheme.equalsIgnoreCase( details.getType() );
  }

  @Override
  public Path mapPath( Path pvfsPath ) {
    validatePath( pvfsPath );
    String[] splitPath = pvfsPath.toUri().getPath().split( "/" );

    Preconditions.checkArgument( splitPath.length > 0 );
    String bucket = accountFQDN;
    String path = SEPARATOR + Arrays.stream( splitPath ).skip( 1 ).collect( Collectors.joining( SEPARATOR ) );
    try {
      return new Path( new URI( scheme, bucket, path, null ) );
    } catch ( URISyntaxException e ) {
      throw new IllegalStateException( e );
    }
  }

  @Override
  public Path mapPath( Path pvfsPath, Path realFsPath ) {
    URI uri = realFsPath.toUri();
    String userInfo = uri.getUserInfo();
    if ( uri.getUserInfo() == null ) {
      userInfo = "";
    }
    return new Path( pvfsPath.toUri().getScheme(),
      getConnectionName( pvfsPath ), "/" + userInfo + uri.getPath() );
  }

  @Override
  public Configuration conf( Path pvfsPath ) {
    Configuration config = new Configuration();
    /**
     * Azure Connector configurations can be found here :
     * https://hadoop.apache.org/docs/r2.8.0/hadoop-azure-datalake/index.html
     */
    config.set( "fs.adl.impl", "org.apache.hadoop.fs.adl.AdlFileSystem" );
    config.set( "fs.AbstractFileSystem.adl.impl", "org.apache.hadoop.fs.adl.Adl" );
    if ( !isNullOrEmpty( clientId ) && !isNullOrEmpty( clientSecret ) && !isNullOrEmpty( authTokenEndpoint ) ) {
      config.set( AZURE_AUTH_TYPE , AdlConfKeys.TOKEN_PROVIDER_TYPE_CLIENT_CRED );
      config.set( "fs.adl.oauth2.refresh.url", authTokenEndpoint);
      config.set( "fs.adl.oauth2.client.id", clientId );
      config.set( "fs.adl.oauth2.credential", clientSecret );
    }
    return config;
  }

  private boolean isServiceToServiceAuthentication( String clientId, String clientSecret, String authTokenEndpoint ) {
    return !isNullOrEmpty( clientId ) && !isNullOrEmpty( clientSecret ) && !isNullOrEmpty( authTokenEndpoint );
  }

  @Override
  public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    if ( !super.equals( o ) ) {
      return false;
    }
    ADLSGen1Conf adlsConf = ( ADLSGen1Conf ) o;
    return Objects.equals( accountFQDN, adlsConf.accountFQDN );
  }

  @Override
  public int hashCode() {
    return Objects.hash( super.hashCode(), accountFQDN );
  }
}

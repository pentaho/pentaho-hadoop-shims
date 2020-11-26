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

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
//import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.pentaho.di.connections.ConnectionDetails;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.google.common.base.Strings.isNullOrEmpty;
import static org.apache.hadoop.fs.Path.SEPARATOR;
import static org.pentaho.hadoop.shim.pvfs.PvfsHadoopBridge.getConnectionName;

public class AzDataLakeGen2Conf extends PvfsConf {

  private final String accountName;
  private String sharedKey;
  private final String scheme;
  private String clientId;
  private String clientSecret;
  private String tenantId;
  private String sasToken;

  public AzDataLakeGen2Conf( ConnectionDetails details ) {
    super( details );
    scheme = new SecureAzureBlobFileSystem().getScheme();
    accountName = details.getProperties().get( "accountName" );
    if ( isSharedKeyAuthentication( details.getProperties().get( "sharedKey" ) ) ) {
      sharedKey = details.getProperties().get( "sharedKey" );
    } else if ( isAzureADAuthentication( details.getProperties().get( "clientId" ), details.getProperties().get( "clientSecret" ), details.getProperties().get( "tenantId" ) ) ) {
      clientId = details.getProperties().get( "clientId" );
      clientSecret = details.getProperties().get( "clientSecret" );
      tenantId = details.getProperties().get( "tenantId" );
    } else if ( isSASTokenAuthentication( details.getProperties().get( "sasToken" ) ) ) {
      sasToken = details.getProperties().get( "sasToken" );
    }
  }

  @Override public boolean supportsConnection() {
    return scheme.equalsIgnoreCase( details.getType() );
  }

  @Override public Path mapPath( Path pvfsPath ) {
    validatePath( pvfsPath );
    String[] splitPath = pvfsPath.toUri().getPath().split( "/" );

    Preconditions.checkArgument( splitPath.length > 0 );
    String bucket = splitPath[1] + "@" + accountName + ".dfs.core.windows.net";
    String path = SEPARATOR + Arrays.stream( splitPath ).skip( 2 ).collect( Collectors.joining( SEPARATOR ) );
    try {
      return new Path( new URI( scheme, bucket, path, null ) );
    } catch ( URISyntaxException e ) {
      throw new IllegalStateException( e );
    }
  }

  @Override public Path mapPath( Path pvfsPath, Path realFsPath ) {
    URI uri = realFsPath.toUri();
    return new Path( pvfsPath.toUri().getScheme(),
            getConnectionName( pvfsPath ), "/" + uri.getUserInfo() + uri.getPath() );
  }

  @Override public Configuration conf( Path pvfsPath ) {
    Configuration config = new Configuration();
    /**
     * Azure Connector configurations can be found here :
     * https://github.com/apache/hadoop/blob/51598d8b1be20726b744ce29928684784061f8cf/hadoop-tools/hadoop-azure/src/site/markdown/testing_azure.md
     * https://hadoop.apache.org/docs/r3.2.0/hadoop-project-dist/hadoop-common/core-default.xml
     */
    config.set( "fs.abfss.impl", "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem" );
    config.set( "fs.AbstractFileSystem.abfss.impl", "org.apache.hadoop.fs.azurebfs.Abfss" );
//    config.set(ConfigurationKeys.FS_AZURE_ACCOUNT_KEY_PROPERTY_NAME, accountName);

    config.set( "fs.azure.abfss.account.name", accountName + ".dfs.core.windows.net" );
    if ( !isNullOrEmpty( sharedKey ) ) {
      config.set( "fs.azure.account.auth.type." + accountName + ".dfs.core.windows.net", AuthType.SharedKey.name() );
      config.set( "fs.azure.account.key." + accountName + ".dfs.core.windows.net", sharedKey );
    } else if ( !isNullOrEmpty( clientId ) && !isNullOrEmpty( clientSecret ) && !isNullOrEmpty( tenantId ) ) {
      config.set( "fs.azure.account.auth.type." + accountName + ".dfs.core.windows.net", AuthType.OAuth.name() );
      config.set( "fs.azure.account.oauth.provider.type." + accountName + ".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider" );
      config.set( "fs.azure.account.oauth2.client.endpoint." + accountName + ".dfs.core.windows.net", "https://login.microsoftonline.com/" + tenantId + "/oauth2/token" );
      config.set( "fs.azure.account.oauth2.client.id." + accountName + ".dfs.core.windows.net", clientId );
      config.set( "fs.azure.account.oauth2.client.secret." + accountName + ".dfs.core.windows.net", clientSecret );
    } else if ( !isNullOrEmpty( sasToken ) ) {
      config.set( "fs.azure.account.auth.type." + accountName + ".dfs.core.windows.net", AuthType.SAS.name() );
      config.set( "fs.azure.sas.token.provider.type", "org.pentaho.hadoop.shim.pvfs.conf.providers.PentahoAzureSasTokenProvider" );
      config.set( "fs.azure.sas.token", sasToken.substring( 1 ) );
    }
//    config.set(ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, AuthType.SharedKey.name());
    config.set( "fs.azure.secure.mode", "false" );
    config.set( "fs.azure.local.sas.key.mode", "false" );
    config.set( "fs.azure.enable.check.access", "true" );
    config.set( "fs.abfss.impl.disable.cache", "true" ); // caching managed by PvfsHadoopBridge

    config.set( "fs.abfss.buffer.dir", System.getProperty( "java.io.tmpdir" ) );
    return config;
  }

  private boolean isSharedKeyAuthentication( String sharedKey ) {
    return !isNullOrEmpty( sharedKey );
  }

  private boolean isAzureADAuthentication( String clientId, String clientSecret, String tenantId ) {
    return !isNullOrEmpty( clientId ) && !isNullOrEmpty( clientSecret ) && !isNullOrEmpty( tenantId );
  }

  private boolean isSASTokenAuthentication( String sasToken ) {
    return !isNullOrEmpty( sasToken );
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
    AzDataLakeGen2Conf adlsConf = (AzDataLakeGen2Conf) o;
    return Objects.equals( accountName, adlsConf.accountName );
  }

  @Override public int hashCode() {
    return Objects.hash( super.hashCode(), accountName );
  }
}

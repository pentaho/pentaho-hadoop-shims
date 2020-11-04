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

import static org.apache.hadoop.fs.Path.SEPARATOR;
import static org.pentaho.hadoop.shim.pvfs.PvfsHadoopBridge.getConnectionName;

public class AzDataLakeGen2Conf extends PvfsConf {

  private final String accountName;
  private final String sharedKey;
  private final String scheme;

  public AzDataLakeGen2Conf( ConnectionDetails details ) {
    super( details );
    scheme = new SecureAzureBlobFileSystem().getScheme();
    accountName = details.getProperties().get( "accountName" );
    sharedKey = details.getProperties().get( "sharedKey" );
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
//            fileName = "abfss://devcontainer@hitachidevus/renamedTestFolder/job-inputs_orc_orc_snappy";
    try {
      return new Path( new URI( scheme, bucket, path, null ) );
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
    Configuration config = new Configuration();
    // https://github.com/apache/hadoop/blob/51598d8b1be20726b744ce29928684784061f8cf/hadoop-tools/hadoop-azure/src/site/markdown/testing_azure.md
    config.set( "fs.abfs.impl", "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem" );
    config.set("fs.AbstractFileSystem.abfs.impl", "org.apache.hadoop.fs.azurebfs.Abfs");
//    config.set(ConfigurationKeys.FS_AZURE_ACCOUNT_KEY_PROPERTY_NAME, accountName);

    config.set("fs.azure.abfs.account.name", accountName + ".dfs.core.windows.net");
    config.set( "fs.azure.account.auth.type." + accountName + ".dfs.core.windows.net", "SharedKey" );
    config.set( "fs.azure.account.key." + accountName + ".dfs.core.windows.net", sharedKey );
//    config.set(ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, AuthType.SharedKey.name());
    config.set("fs.azure.secure.mode", "false");
    config.set("fs.azure.local.sas.key.mode", "false");
    config.set("fs.azure.enable.check.access", "true");
    //TODO: Add various other Auth modes and configurations
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
    AzDataLakeGen2Conf adlsConf = (AzDataLakeGen2Conf) o;
    return Objects.equals( accountName, adlsConf.accountName );
  }

  @Override public int hashCode() {
    return Objects.hash( super.hashCode(), accountName );
  }
}

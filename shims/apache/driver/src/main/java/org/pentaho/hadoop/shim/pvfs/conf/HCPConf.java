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

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Constants;
import org.pentaho.di.connections.ConnectionDetails;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Base64;
import java.util.Map;

import static org.pentaho.di.core.util.Utils.isEmpty;
import static org.pentaho.hadoop.shim.pvfs.PvfsHadoopBridge.getConnectionName;


public class HCPConf extends PvfsConf {

  public HCPConf( ConnectionDetails details ) {
    super( details );
  }

  @Override public boolean supportsConnection() {
    return "hcp".equalsIgnoreCase( details.getType() );
  }

  @Override public Path mapPath( Path pvfsPath ) {
    validatePath( pvfsPath );
    Map<String, String> props = details.getProperties();
    String namespace = props.get( "namespace" );
    try {
      return new Path(
        new URI( "s3a", namespace, pvfsPath.toUri().getPath(), null ) );
    } catch ( URISyntaxException e ) {
      throw new IllegalStateException( e );
    }
  }

  @Override public Path mapPath( Path pvfsPath, Path realFsPath ) {
    URI uri = realFsPath.toUri();
    return new Path( pvfsPath.toUri().getScheme(),
      getConnectionName( pvfsPath ), "/" + uri.getPath() );
  }

  @Override public Configuration conf( Path pvfsPath ) {
    validatePath( pvfsPath );
    Configuration conf = new Configuration();
    Map<String, String> props = details.getProperties();

    String port = props.get( "port" );
    String hostPort = props.get( "host" ) + ( port == null ? "" : ":" + port );
    String username = props.get( "username" );
    String proxyHost = props.get( "proxyHost" );
    String proxyPort = props.get( "proxyPort" );
    boolean acceptSelfSignedCertificates = Boolean.parseBoolean( props.get( "acceptSelfSignedCertificate" ) );

    if ( !isEmpty( proxyHost ) ) {
      conf.set( "fs.s3a.proxy.host", proxyHost );
      conf.set( "fs.s3a.proxy.port", proxyPort );
    }

    conf.set( "fs.s3a.access.key", Base64.getEncoder().encodeToString( username.getBytes() ) );
    conf.set( "fs.s3a.secret.key", DigestUtils.md5Hex( props.get( "password" ) ) );
    conf.set( "fs.s3a.endpoint", props.get( "tenant" ) + "." + hostPort );
    conf.set( "fs.s3a.signing-algorithm", "S3SignerType" );
    conf.set( "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem" );
    conf.set( "fs.s3a.connection.ssl.enabled", "true" );
    conf.set( "fs.s3a.attempts.maximum", "3" );

    conf.set( "fs.s3a.impl.disable.cache", "true" ); // caching managed by PvfsHadoopBridge

    if ( acceptSelfSignedCertificates ) {
      conf.set( Constants.S3_CLIENT_FACTORY_IMPL, "org.pentaho.hadoop.shim.pvfs.SelfSignedS3ClientFactory" );
    }
    return conf;
  }
}

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

    String port = getVar( props, "port" );
    String hostPort = getVar( props, "host" ) + ( port == null ? "" : ":" + port );
    String username = getVar( props, "username" );
    String proxyHost = getVar( props, "proxyHost" );
    String proxyPort = getVar( props, "proxyPort" );
    boolean acceptSelfSignedCertificates = Boolean.parseBoolean( props.get( "acceptSelfSignedCertificate" ) );

    if ( !isEmpty( proxyHost ) ) {
      conf.set( "fs.s3a.proxy.host", proxyHost );
      conf.set( "fs.s3a.proxy.port", proxyPort );
    }

    conf.set( "fs.s3a.access.key", Base64.getEncoder().encodeToString( username.getBytes() ) );
    conf.set( "fs.s3a.secret.key", DigestUtils.md5Hex( props.get( "password" ) ) );
    conf.set( "fs.s3a.endpoint", getVar( props,"tenant" ) + "." + hostPort );
    conf.set( "fs.s3a.signing-algorithm", "S3SignerType" );
    conf.set( "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem" );
    conf.set( "fs.s3a.connection.ssl.enabled", "true" );
    conf.set( "fs.s3a.attempts.maximum", "3" );

    conf.set( "fs.s3a.impl.disable.cache", "true" ); // caching managed by PvfsHadoopBridge

    // TODO Defect BACKLOG-42556 was opened to track this issue
    if ( acceptSelfSignedCertificates ) {
      conf.set( Constants.S3_CLIENT_FACTORY_IMPL, "org.pentaho.hadoop.shim.pvfs.SelfSignedS3ClientFactory" );
    }
    return conf;
  }
}

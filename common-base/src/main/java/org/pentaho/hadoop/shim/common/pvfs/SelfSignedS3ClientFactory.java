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


package org.pentaho.hadoop.shim.common.pvfs;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.thirdparty.apache.http.conn.ssl.SSLConnectionSocketFactory;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.s3a.DefaultS3ClientFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.ssl.SSLContextBuilder;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

public class SelfSignedS3ClientFactory extends DefaultS3ClientFactory {
  private final HostnameVerifier allowAllHosts = new NoopHostnameVerifier();
  @VisibleForTesting final SSLConnectionSocketFactory connectionFactory = selfSignedSSLConnFactory();

  // TODO Orc/Parquet input/output steps are not working with this implementation
  // TODO Defect BACKLOG-42556 was opened to track this issue
  // the s3a impl (3.1.x) is dependent on a mutable client, so we have to use the
  // deprecated constructor rather than the builder, at least until s3a is updated.
  /*
  @SuppressWarnings( { "deprecation", "squid:CallToDeprecatedMethod" } )
  @Override protected AmazonS3 newAmazonS3Client( AWSCredentialsProvider credentials, ClientConfiguration awsConf ) {
    awsConf.getApacheHttpClientConfig().setSslSocketFactory( connectionFactory );
    return new AmazonS3Client( credentials, awsConf );
  }
  */

  private SSLConnectionSocketFactory selfSignedSSLConnFactory() {
    SSLContext sslContext;
    try {
      sslContext = SSLContextBuilder.create().loadTrustMaterial( new TrustSelfSignedStrategy() ).build();
    } catch ( NoSuchAlgorithmException | KeyStoreException | KeyManagementException e ) {
      throw new IllegalStateException( e );
    }
    return new SSLConnectionSocketFactory( sslContext, allowAllHosts );
  }
}

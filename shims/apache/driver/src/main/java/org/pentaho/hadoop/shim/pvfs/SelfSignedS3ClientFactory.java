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

package org.pentaho.hadoop.shim.pvfs;

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


  @SuppressWarnings( { "deprecation", "squid:CallToDeprecatedMethod" } )
  // the s3a impl (3.1.x) is dependent on a mutable client, so we have to use the
  // deprecated constructor rather than the builder, at least until s3a is updated.
  @Override protected AmazonS3 newAmazonS3Client( AWSCredentialsProvider credentials, ClientConfiguration awsConf ) {
    awsConf.getApacheHttpClientConfig().setSslSocketFactory( connectionFactory );
    return new AmazonS3Client( credentials, awsConf );
  }

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

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


package org.pentaho.hadoop.shim.pvfs;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertFalse;

@RunWith( MockitoJUnitRunner.class )
public class SelfSignedS3ClientFactoryTest {

  @Test public void newAmazonS3Client() {
    AWSCredentialsProvider provider = new AWSStaticCredentialsProvider(
      new BasicAWSCredentials( "accessKey", "secretKey" ) );
    ClientConfiguration conf = new ClientConfiguration();
    SelfSignedS3ClientFactory selfSignedS3ClientFactory = new SelfSignedS3ClientFactory();
    AmazonS3 s3Client =
      selfSignedS3ClientFactory
        .newAmazonS3Client( provider, conf );
    assertFalse( s3Client == null );
    assertThat( conf.getApacheHttpClientConfig().getSslSocketFactory(),
      equalTo( selfSignedS3ClientFactory.connectionFactory ) );
  }
}

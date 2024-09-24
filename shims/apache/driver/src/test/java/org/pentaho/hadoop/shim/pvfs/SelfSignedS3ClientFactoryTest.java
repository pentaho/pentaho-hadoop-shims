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

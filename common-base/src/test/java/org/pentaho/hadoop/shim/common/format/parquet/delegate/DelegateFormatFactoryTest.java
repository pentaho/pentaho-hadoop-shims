/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/


package org.pentaho.hadoop.shim.common.format.parquet.delegate;

import org.junit.Test;
import org.mockito.MockedConstruction;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.common.format.parquet.delegate.apache.PentahoApacheInputFormat;
import org.pentaho.hadoop.shim.common.format.parquet.delegate.apache.PentahoApacheOutputFormat;
import org.pentaho.hadoop.shim.common.format.parquet.delegate.twitter.PentahoTwitterInputFormat;
import org.pentaho.hadoop.shim.common.format.parquet.delegate.twitter.PentahoTwitterOutputFormat;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

public class DelegateFormatFactoryTest {

  @Test
  public void testNullNamedClusterUsesApacheFormats() {
    assertFormats( null, true );
  }

  @Test
  public void testNullShimIdentifierUsesApacheFormats() {
    assertFormats( namedCluster( null ), true );
  }

  @Test
  public void testApacheShimUsesApacheFormats() {
    assertFormats( namedCluster( "apachevanilla" ), true );
  }

  @Test
  public void testLegacyCdhShimUsesTwitterFormats() {
    assertFormats( namedCluster( "cdh514" ), false );
  }

  private static NamedCluster namedCluster( String shimIdentifier ) {
    NamedCluster namedCluster = mock( NamedCluster.class );
    when( namedCluster.getShimIdentifier() ).thenReturn( shimIdentifier );
    return namedCluster;
  }

  private static void assertFormats( NamedCluster namedCluster, boolean expectApache ) {
    int apache = expectApache ? 1 : 0;
    int twitter = expectApache ? 0 : 1;
    try ( MockedConstruction<PentahoApacheInputFormat> apacheInput = mockConstruction( PentahoApacheInputFormat.class );
          MockedConstruction<PentahoApacheOutputFormat> apacheOutput =
            mockConstruction( PentahoApacheOutputFormat.class );
          MockedConstruction<PentahoTwitterInputFormat> twitterInput =
            mockConstruction( PentahoTwitterInputFormat.class );
          MockedConstruction<PentahoTwitterOutputFormat> twitterOutput =
            mockConstruction( PentahoTwitterOutputFormat.class ) ) {
      DelegateFormatFactory.getInputFormatInstance( namedCluster );
      DelegateFormatFactory.getOutputFormatInstance( namedCluster );

      assertEquals( apache, apacheInput.constructed().size() );
      assertEquals( apache, apacheOutput.constructed().size() );
      assertEquals( twitter, twitterInput.constructed().size() );
      assertEquals( twitter, twitterOutput.constructed().size() );
    }
  }
}

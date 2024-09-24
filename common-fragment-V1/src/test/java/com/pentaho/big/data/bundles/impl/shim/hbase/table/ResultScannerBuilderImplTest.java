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

package com.pentaho.big.data.bundles.impl.shim.hbase.table;

import com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool.HBaseConnectionPool;
import com.pentaho.big.data.bundles.impl.shim.hbase.meta.HBaseValueMetaInterfaceFactoryImpl;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.hadoop.shim.api.internal.hbase.HBaseBytesUtilShim;

import java.nio.charset.Charset;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Created by bryan on 2/29/16.
 */
public class ResultScannerBuilderImplTest {
  public static final Charset UTF_8 = Charset.forName( "UTF-8" );
  private HBaseConnectionPool hBaseConnectionPool;
  private HBaseValueMetaInterfaceFactoryImpl hBaseValueMetaInterfaceFactory;
  private HBaseBytesUtilShim hBaseBytesUtilShim;
  private String testTableName;
  private int caching;
  private byte[] keyLowerBound;
  private byte[] keyUpperBound;
  private ResultScannerBuilderImpl resultScannerBuilder;

  @Before
  public void setup() {
    hBaseConnectionPool = mock( HBaseConnectionPool.class );
    hBaseValueMetaInterfaceFactory = mock( HBaseValueMetaInterfaceFactoryImpl.class );
    hBaseBytesUtilShim = mock( HBaseBytesUtilShim.class );
    testTableName = "testTableName";
    caching = 11;
    keyLowerBound = "lower".getBytes( UTF_8 );
    keyUpperBound = "upper".getBytes( UTF_8 );
    init();
  }

  private void init() {
    resultScannerBuilder =
      new ResultScannerBuilderImpl( hBaseConnectionPool, hBaseValueMetaInterfaceFactory, hBaseBytesUtilShim,
        testTableName, caching, keyLowerBound, keyUpperBound );
  }

  @Test
  public void testCachingConstructorArg() {
    assertEquals( caching, resultScannerBuilder.getCaching() );
    caching = 12;
    init();
    assertEquals( caching, resultScannerBuilder.getCaching() );
  }
}

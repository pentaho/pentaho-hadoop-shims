/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package com.pentaho.big.data.bundles.impl.shim.hbase.table;

import com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool.HBaseConnectionPool;
import com.pentaho.big.data.bundles.impl.shim.hbase.meta.HBaseValueMetaInterfaceFactoryImpl;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.hbase.shim.spi.HBaseBytesUtilShim;

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

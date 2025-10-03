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


package com.pentaho.big.data.bundles.impl.shim.hbase.table;

import com.pentaho.big.data.bundles.impl.shim.hbase.HBaseConnectionWrapper;
import com.pentaho.big.data.bundles.impl.shim.hbase.ResultImpl;
import com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool.HBaseConnectionHandle;
import org.apache.hadoop.hbase.client.Result;
import org.pentaho.hadoop.shim.api.hbase.table.ResultScanner;
import org.pentaho.hadoop.shim.api.internal.hbase.HBaseBytesUtilShim;

import java.io.IOException;

/**
 * Created by bryan on 1/25/16.
 */
public class ResultScannerImpl implements ResultScanner {
  private final HBaseConnectionHandle hBaseConnectionHandle;
  private final HBaseConnectionWrapper hBaseConnectionWrapper;
  private final HBaseBytesUtilShim hBaseBytesUtilShim;

  public ResultScannerImpl( HBaseConnectionHandle hBaseConnectionHandle, HBaseBytesUtilShim hBaseBytesUtilShim ) {
    this.hBaseConnectionHandle = hBaseConnectionHandle;
    this.hBaseBytesUtilShim = hBaseBytesUtilShim;
    hBaseConnectionWrapper = hBaseConnectionHandle.getConnection();
  }

  @Override public ResultImpl next() throws IOException {
    try {
      if ( !hBaseConnectionWrapper.resultSetNextRow() ) {
        return null;
      }
      return new ResultImpl( (Result) hBaseConnectionWrapper.getCurrentResult(),
        hBaseBytesUtilShim );
    } catch ( Exception e ) {
      throw new IOException( e );
    }
  }

  @Override public void close() throws IOException {
    hBaseConnectionHandle.close();
  }
}

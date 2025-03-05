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


package com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool;

import java.io.IOException;

/**
 * Created by bryan on 1/25/16.
 */
public class HBaseConnectionHandleImpl implements HBaseConnectionHandle {
  private final HBaseConnectionPool hBaseConnectionPool;
  private HBaseConnectionPoolConnection hBaseConnection;

  public HBaseConnectionHandleImpl( HBaseConnectionPool hBaseConnectionPool,
                                    HBaseConnectionPoolConnection hBaseConnection ) {
    this.hBaseConnectionPool = hBaseConnectionPool;
    this.hBaseConnection = hBaseConnection;
  }

  @Override public HBaseConnectionPoolConnection getConnection() {
    return hBaseConnection;
  }

  @Override public void close() throws IOException {
    HBaseConnectionPoolConnection hBaseConnection = this.hBaseConnection;
    this.hBaseConnection = null;
    hBaseConnectionPool.releaseConnection( hBaseConnection );
  }
}

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


package com.pentaho.big.data.bundles.impl.shim.hbase;

import org.apache.hadoop.hbase.client.Result;
import org.pentaho.hadoop.shim.spi.HBaseConnection;

/**
 * Created by bryan on 2/2/16.
 */
public interface HBaseConnectionTestImpls {
  abstract class HBaseConnectionWithResultField implements HBaseConnection {
    private Result m_currentResultSetRow;

    public static abstract class Subclass extends HBaseConnectionWithResultField {
    }
  }

  abstract class HBaseConnectionWithMismatchedDelegate implements HBaseConnection {
    private Object delegate;
  }

  abstract class HBaseConnectionWithPublicDelegate implements HBaseConnection {
    public Object delegate;
  }
}

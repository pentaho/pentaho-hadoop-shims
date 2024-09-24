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

package com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool;

import com.pentaho.big.data.bundles.impl.shim.hbase.HBaseConnectionWrapper;

import java.io.Closeable;

/**
 * Created by bryan on 1/25/16.
 */
public interface HBaseConnectionHandle extends Closeable {
  HBaseConnectionWrapper getConnection();
}

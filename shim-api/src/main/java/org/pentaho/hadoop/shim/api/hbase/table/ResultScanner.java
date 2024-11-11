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


package org.pentaho.hadoop.shim.api.hbase.table;

import org.pentaho.hadoop.shim.api.hbase.Result;

import java.io.Closeable;
import java.io.IOException;

/**
 * Created by bryan on 1/19/16.
 */
public interface ResultScanner extends Closeable {
  Result next() throws IOException;
}

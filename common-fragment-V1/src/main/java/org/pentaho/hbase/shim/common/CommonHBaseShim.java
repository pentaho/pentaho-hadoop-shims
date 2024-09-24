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

package org.pentaho.hbase.shim.common;

import org.pentaho.hadoop.shim.ShimVersion;
import org.pentaho.hadoop.shim.spi.HBaseConnection;
import org.pentaho.hadoop.shim.spi.HBaseShim;

/**
 * Concrete implementation of HBaseShim suitable for use with Apache HBase 0.90.x.
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
public class CommonHBaseShim implements HBaseShim {

  @Override
  public ShimVersion getVersion() {
    return new ShimVersion( 1, 0 );
  }

  public HBaseConnection getHBaseConnection() {
    return new CommonHBaseConnection();
  }
}

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

import org.apache.hadoop.conf.Configuration;
import org.pentaho.hadoop.shim.spi.HBaseConnection;
import org.pentaho.hbase.shim.common.wrapper.HBaseShimInterface;

public class HBaseShimImpl extends CommonHBaseShim implements HBaseShimInterface {

  public HBaseConnection getHBaseConnection() {
    return new HBaseConnectionImpl();
  }

  @Override
  public void setInfo( Configuration configuration ) {
    // noop
  }

}

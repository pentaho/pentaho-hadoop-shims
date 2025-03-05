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

package org.pentaho.hadoop.hbase.factory;

import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.pentaho.hbase.factory.HBasePut;

public class HBase10Put implements HBasePut {
  Put put;

  HBase10Put( byte[] key ) {
    this.put = new Put( key );
  }

  @Override
  public void setWriteToWAL( boolean writeToWAL ) throws Exception {
    put.getClass().getMethod( "setDurability", Durability.class )
      .invoke( put, ( writeToWAL ? Durability.USE_DEFAULT : Durability.SKIP_WAL ) );
  }

  @Override
  public void addColumn( byte[] colFamily, byte[] colName, byte[] colValue ) {
    put.addColumn( colFamily, colName, colValue );
  }

  Put getPut() {
    return put;
  }

}

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

import com.pentaho.big.data.bundles.impl.shim.hbase.BatchHBaseConnectionOperation;
import com.pentaho.big.data.bundles.impl.shim.hbase.HBaseConnectionOperation;
import com.pentaho.big.data.bundles.impl.shim.hbase.HBaseConnectionWrapper;
import com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool.HBaseConnectionHandle;
import org.pentaho.hadoop.shim.api.hbase.table.HBasePut;
import org.pentaho.hadoop.shim.api.internal.hbase.HBaseValueMeta;

import java.io.IOException;

/**
 * Created by bryan on 1/26/16.
 */
public class HBasePutImpl implements HBasePut {
  private final HBaseConnectionHandle hBaseConnectionHandle;
  private final BatchHBaseConnectionOperation batchHBaseConnectionOperation;
  private boolean writeToWAL;

  public HBasePutImpl( final byte[] key, HBaseConnectionHandle hBaseConnectionHandle ) {
    this.hBaseConnectionHandle = hBaseConnectionHandle;
    batchHBaseConnectionOperation = new BatchHBaseConnectionOperation();
    batchHBaseConnectionOperation.addOperation( new HBaseConnectionOperation() {
      @Override public void perform( HBaseConnectionWrapper hBaseConnectionWrapper ) throws IOException {
        try {
          hBaseConnectionWrapper.newTargetTablePut( key, writeToWAL );
        } catch ( Exception e ) {
          throw new IOException( e );
        }
      }
    } );
  }

  @Override public void setWriteToWAL( boolean writeToWAL ) {
    this.writeToWAL = writeToWAL;
  }

  @Override public void addColumn( final String columnFamily, final String columnName, final boolean colNameIsBinary,
                                   final byte[] colValue )
    throws IOException {
    batchHBaseConnectionOperation.addOperation( new HBaseConnectionOperation() {
      @Override public void perform( HBaseConnectionWrapper hBaseConnectionWrapper ) throws IOException {
        try {
          hBaseConnectionWrapper.addColumnToTargetPut( columnFamily, columnName, colNameIsBinary, colValue );
        } catch ( Exception e ) {
          throw new IOException( e );
        }
      }
    } );
  }

  @Override public String createColumnName( String... parts ) {
    StringBuilder result = new StringBuilder();
    for ( String part : parts ) {
      result.append( part );
      result.append( HBaseValueMeta.SEPARATOR );
    }
    if ( result.length() > 0 ) {
      result.setLength( result.length() - HBaseValueMeta.SEPARATOR.length() );
    }
    return result.toString();
  }

  @Override public void execute() throws IOException {
    HBaseConnectionWrapper connection = hBaseConnectionHandle.getConnection();
    batchHBaseConnectionOperation.perform( connection );
    try {
      connection.executeTargetTablePut();
    } catch ( Exception e ) {
      throw new IOException( e );
    }
  }
}

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


package com.pentaho.big.data.bundles.impl.shim.hbase.table;

import com.google.common.annotations.VisibleForTesting;
import com.pentaho.big.data.bundles.impl.shim.hbase.BatchHBaseConnectionOperation;
import com.pentaho.big.data.bundles.impl.shim.hbase.HBaseConnectionOperation;
import com.pentaho.big.data.bundles.impl.shim.hbase.HBaseConnectionWrapper;
import com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool.HBaseConnectionHandle;
import com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool.HBaseConnectionPool;
import com.pentaho.big.data.bundles.impl.shim.hbase.meta.HBaseValueMetaInterfaceFactoryImpl;
import com.pentaho.big.data.bundles.impl.shim.hbase.meta.HBaseValueMetaInterfaceImpl;

import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.hadoop.shim.api.hbase.mapping.ColumnFilter;
import org.pentaho.hadoop.shim.api.hbase.meta.HBaseValueMetaInterface;
import org.pentaho.hadoop.shim.api.hbase.table.ResultScanner;
import org.pentaho.hadoop.shim.api.hbase.table.ResultScannerBuilder;
import org.pentaho.hadoop.shim.api.internal.hbase.HBaseBytesUtilShim;

import java.io.IOException;

/**
 * Created by bryan on 1/25/16.
 */
public class ResultScannerBuilderImpl implements ResultScannerBuilder {
  private final HBaseConnectionPool hBaseConnectionPool;
  private final HBaseValueMetaInterfaceFactoryImpl hBaseValueMetaInterfaceFactory;
  private final HBaseBytesUtilShim hBaseBytesUtilShim;
  private final BatchHBaseConnectionOperation batchHBaseConnectionOperation;
  private int caching = 0;
  private String tableName;

  public ResultScannerBuilderImpl( HBaseConnectionPool hBaseConnectionPool,
                                   HBaseValueMetaInterfaceFactoryImpl hBaseValueMetaInterfaceFactory,
                                   HBaseBytesUtilShim hBaseBytesUtilShim, String tableName,
                                   final int caching,
                                   final byte[] keyLowerBound,
                                   final byte[] keyUpperBound ) {
    this.hBaseConnectionPool = hBaseConnectionPool;
    this.hBaseValueMetaInterfaceFactory = hBaseValueMetaInterfaceFactory;
    this.hBaseBytesUtilShim = hBaseBytesUtilShim;
    this.batchHBaseConnectionOperation = new BatchHBaseConnectionOperation();
    this.tableName = tableName;
    this.caching = caching;
    batchHBaseConnectionOperation.addOperation( new HBaseConnectionOperation() {
      @Override public void perform( HBaseConnectionWrapper hBaseConnectionWrapper ) throws IOException {
        try {
          hBaseConnectionWrapper
            .newSourceTableScan( keyLowerBound, keyUpperBound, ResultScannerBuilderImpl.this.caching );
        } catch ( Exception e ) {
          throw new IOException( e );
        }
      }
    } );
  }

  @Override
  public void addColumnToScan( final String colFamilyName, final String colName, final boolean colNameIsBinary )
    throws IOException {
    batchHBaseConnectionOperation.addOperation( new HBaseConnectionOperation() {
      @Override public void perform( HBaseConnectionWrapper hBaseConnectionWrapper ) throws IOException {
        try {
          hBaseConnectionWrapper.addColumnToScan( colFamilyName, colName, colNameIsBinary );
        } catch ( Exception e ) {
          throw new IOException( e );
        }
      }
    } );
  }

  @Override public void addColumnFilterToScan( ColumnFilter cf, HBaseValueMetaInterface columnMeta,
                                               final VariableSpace vars,
                                               final boolean matchAny ) throws IOException {
    final org.pentaho.hadoop.shim.api.internal.hbase.ColumnFilter columnFilter =
      new org.pentaho.hadoop.shim.api.internal.hbase.ColumnFilter( cf.getFieldAlias() );
    columnFilter.setFormat( cf.getFormat() );
    columnFilter.setConstant( cf.getConstant() );
    columnFilter.setSignedComparison( cf.getSignedComparison() );
    columnFilter.setFieldType( cf.getFieldType() );
    columnFilter.setComparisonOperator(
      org.pentaho.hadoop.shim.api.internal.hbase.ColumnFilter.ComparisonType
        .valueOf( cf.getComparisonOperator().name() ) );
    final HBaseValueMetaInterfaceImpl hBaseValueMetaInterface = hBaseValueMetaInterfaceFactory.copy( columnMeta );
    batchHBaseConnectionOperation.addOperation( new HBaseConnectionOperation() {
      @Override public void perform( HBaseConnectionWrapper hBaseConnectionWrapper ) throws IOException {
        try {
          hBaseConnectionWrapper
            .addColumnFilterToScan( columnFilter, hBaseValueMetaInterface, vars, matchAny );
        } catch ( Exception e ) {
          throw new IOException( e );
        }
      }
    } );
  }

  @Override public void setCaching( int cacheSize ) {
    this.caching = cacheSize;
  }

  @VisibleForTesting
  int getCaching() {
    return caching;
  }

  @Override public ResultScanner build() throws  IOException {
    HBaseConnectionHandle connectionHandle = hBaseConnectionPool.getConnectionHandle( tableName );
    batchHBaseConnectionOperation.perform( connectionHandle.getConnection() );
    try {
      connectionHandle.getConnection().executeSourceTableScan();
    } catch ( Exception e ) {
      throw new IOException( e );
    }
    return getResultScanner( connectionHandle );
  }

  protected ResultScanner getResultScanner(HBaseConnectionHandle connectionHandle) {
    return new ResultScannerImpl( connectionHandle, hBaseBytesUtilShim );
  }
}

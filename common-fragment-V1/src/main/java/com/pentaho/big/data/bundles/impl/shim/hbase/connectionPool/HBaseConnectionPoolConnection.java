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
import org.pentaho.hadoop.shim.spi.HBaseConnection;

import java.util.Properties;

/**
 * Created by bryan on 1/26/16.
 */
public class HBaseConnectionPoolConnection extends HBaseConnectionWrapper {
  private String sourceTable;
  private String targetTable;
  private Properties targetTableProperties;

  public HBaseConnectionPoolConnection( HBaseConnection delegate ) {
    super( delegate );
  }

  @Override public void newSourceTable( String s ) throws Exception {
    throw new UnsupportedOperationException( "Only the pool should call this" );
  }

  @Override public void closeSourceTable() throws Exception {
    throw new UnsupportedOperationException( "Only the pool should call this" );
  }

  @Override public void newTargetTable( String s, Properties properties ) throws Exception {
    throw new UnsupportedOperationException( "Only the pool should call this" );
  }

  @Override public void closeTargetTable() throws Exception {
    throw new UnsupportedOperationException( "Only the pool should call this" );
  }

  @Override public void close() throws Exception {
    throw new UnsupportedOperationException( "Only the pool should call this" );
  }

  public String getTargetTable() {
    return targetTable;
  }

  public Properties getTargetTableProperties() {
    return targetTableProperties;
  }

  public String getSourceTable() {
    return sourceTable;
  }

  protected void closeInternal() throws Exception {
    closeSourceTableInternal();
    closeTargetTableInternal();
    super.close();
  }

  protected void newTargetTableInternal( String s, Properties properties ) throws Exception {
    super.newTargetTable( s, properties );
    this.targetTable = s;
    this.targetTableProperties = properties;
  }

  protected void newSourceTableInternal( String s ) throws Exception {
    super.newSourceTable( s );
    this.sourceTable = s;
  }

  protected void closeTargetTableInternal() throws Exception {
    this.targetTable = null;
    this.targetTableProperties = null;
    super.closeTargetTable();
  }

  protected void closeSourceTableInternal() throws Exception {
    this.sourceTable = null;
    super.closeSourceTable();
  }
}

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


package com.pentaho.big.data.bundles.impl.shim.hbase;

import com.google.common.annotations.VisibleForTesting;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.internal.hbase.ColumnFilter;
import org.pentaho.hadoop.shim.api.internal.hbase.HBaseBytesUtilShim;
import org.pentaho.hadoop.shim.api.internal.hbase.HBaseValueMeta;
import org.pentaho.hadoop.shim.spi.HBaseConnection;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.NavigableMap;
import java.util.Properties;

/**
 * Created by bryan on 1/25/16.
 */
public class HBaseConnectionWrapper implements HBaseConnection {
  private final HBaseConnection delegate;
  private final HBaseConnection realImpl;
  private final Field resultSetRowField;

  public HBaseConnectionWrapper( HBaseConnection delegate ) {
    this.delegate = delegate;
    this.realImpl = findRealImpl( delegate );

    if ( realImpl != null) {
      resultSetRowField = getResultSetRowField( this.realImpl );
    } else {
      resultSetRowField = null;
    }

    if ( resultSetRowField != null ) {
      resultSetRowField.setAccessible( true );
    }
  }

  @Override public HBaseBytesUtilShim getBytesUtil() throws Exception {
    return delegate.getBytesUtil();
  }

  @Override public void configureConnection( Properties properties, NamedCluster namedCluster, List<String> list )
    throws Exception {
    delegate.configureConnection( properties, namedCluster, list );
  }

  @Override public void checkHBaseAvailable() throws Exception {
    delegate.checkHBaseAvailable();
  }

  @Override public List<String> listTableNames() throws Exception {
    return delegate.listTableNames();
  }

  @Override public boolean tableExists( String s ) throws Exception {
    return delegate.tableExists( s );
  }

  @Override public boolean isTableDisabled( String s ) throws Exception {
    return delegate.isTableDisabled( s );
  }

  @Override public boolean isTableAvailable( String s ) throws Exception {
    return delegate.isTableAvailable( s );
  }

  @Override public void disableTable( String s ) throws Exception {
    delegate.disableTable( s );
  }

  @Override public void enableTable( String s ) throws Exception {
    delegate.enableTable( s );
  }

  @Override public void deleteTable( String s ) throws Exception {
    delegate.deleteTable( s );
  }

  @Override public void executeTargetTableDelete( byte[] bytes ) throws Exception {
    delegate.executeTargetTableDelete( bytes );
  }

  @Override public void createTable( String s, List<String> list, Properties properties ) throws Exception {
    delegate.createTable( s, list, properties );
  }

  @Override public List<String> getTableFamiles( String s ) throws Exception {
    return delegate.getTableFamiles( s );
  }

  @Override public void newSourceTable( String s ) throws Exception {
    delegate.newSourceTable( s );
  }

  @Override public boolean sourceTableRowExists( byte[] bytes ) throws Exception {
    return delegate.sourceTableRowExists( bytes );
  }

  @Override public void newSourceTableScan( byte[] bytes, byte[] bytes1, int i ) throws Exception {
    delegate.newSourceTableScan( bytes, bytes1, i );
  }

  @Override public void newTargetTablePut( byte[] bytes, boolean b ) throws Exception {
    delegate.newTargetTablePut( bytes, b );
  }

  @Override public boolean targetTableIsAutoFlush() throws Exception {
    return delegate.targetTableIsAutoFlush();
  }

  @Override public void executeTargetTablePut() throws Exception {
    delegate.executeTargetTablePut();
  }

  @Override public void flushCommitsTargetTable() throws Exception {
    delegate.flushCommitsTargetTable();
  }

  @Override public void addColumnToTargetPut( String s, String s1, boolean b, byte[] bytes ) throws Exception {
    delegate.addColumnToTargetPut( s, s1, b, bytes );
  }

  @Override public void addColumnFilterToScan( ColumnFilter columnFilter,
                                               HBaseValueMeta hBaseValueMeta,
                                               VariableSpace variableSpace, boolean b ) throws Exception {
    delegate.addColumnFilterToScan( columnFilter, hBaseValueMeta, variableSpace, b );
  }

  @Override public void addColumnToScan( String s, String s1, boolean b ) throws Exception {
    delegate.addColumnToScan( s, s1, b );
  }

  @Override public void executeSourceTableScan() throws Exception {
    delegate.executeSourceTableScan();
  }

  @Override public boolean resultSetNextRow() throws Exception {
    return delegate.resultSetNextRow();
  }

  @Override public byte[] getRowKey( Object o ) throws Exception {
    return delegate.getRowKey( o );
  }

  @Override public byte[] getResultSetCurrentRowKey() throws Exception {
    return delegate.getResultSetCurrentRowKey();
  }

  @Override public byte[] getRowColumnLatest( Object o, String s, String s1, boolean b ) throws Exception {
    return delegate.getRowColumnLatest( o, s, s1, b );
  }

  @Override public boolean checkForHBaseRow( Object o ) {
    return delegate.checkForHBaseRow( o );
  }

  @Override public byte[] getResultSetCurrentRowColumnLatest( String s, String s1, boolean b ) throws Exception {
    return delegate.getResultSetCurrentRowColumnLatest( s, s1, b );
  }

  @Override public NavigableMap<byte[], byte[]> getRowFamilyMap( Object o, String s ) throws Exception {
    return delegate.getRowFamilyMap( o, s );
  }

  @Override public NavigableMap<byte[], byte[]> getResultSetCurrentRowFamilyMap( String s ) throws Exception {
    return delegate.getResultSetCurrentRowFamilyMap( s );
  }

  @Override public NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> getRowMap(
    Object o ) throws Exception {
    return delegate.getRowMap( o );
  }

  @Override public NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> getResultSetCurrentRowMap()
    throws Exception {
    return delegate.getResultSetCurrentRowMap();
  }

  @Override public void closeSourceTable() throws Exception {
    delegate.closeSourceTable();
  }

  @Override public void closeSourceResultSet() throws Exception {
    delegate.closeSourceResultSet();
  }

  @Override public void newTargetTable( String s, Properties properties ) throws Exception {
    delegate.newTargetTable( s, properties );
  }

  @Override public void closeTargetTable() throws Exception {
    delegate.closeTargetTable();
  }

  @Override public boolean isImmutableBytesWritable( Object o ) {
    return delegate.isImmutableBytesWritable( o );
  }

  @Override public void close() throws Exception {
    delegate.close();
  }

  @Override
  public void obtainAuthTokenForJob( org.pentaho.hadoop.shim.api.internal.Configuration conf ) throws Exception {
    delegate.obtainAuthTokenForJob( conf );
  }

  public Object getCurrentResult() {
    try {
      return resultSetRowField.get( realImpl );
    } catch ( IllegalAccessException e ) {
      return null;
    }
  }

  @VisibleForTesting
  static Field getResultSetRowField( Object o ) {
    return getField( o.getClass(), "m_currentResultSetRow" );
  }

  @VisibleForTesting
  static Field getField( Class<?> clazz, String fieldName ) {
    Class<?> current = clazz;
    while ( current != null ) {
      try {
        return current.getDeclaredField( fieldName );
      } catch ( NoSuchFieldException e ) {
        // Ignore
      }
      current = current.getSuperclass();
    }
    return null;
  }

  @VisibleForTesting
  static HBaseConnection findRealImpl( Object hBaseConnection ) {
    Class<?> hBaseConnectionClass = hBaseConnection.getClass();
    if ( Proxy.isProxyClass( hBaseConnectionClass ) ) {
      return unwrapProxy( hBaseConnection );
    } else if ( hBaseConnection instanceof HBaseConnection ) {
      if ( getResultSetRowField( hBaseConnection ) != null ) {
        return (HBaseConnection) hBaseConnection;
      }
      Field delegateField = getField( hBaseConnectionClass, "delegate" );
      if ( delegateField != null ) {
        Object delegate = getFieldValue( delegateField, hBaseConnection );
        if ( delegate instanceof HBaseConnection || Proxy.isProxyClass( delegate.getClass() ) ) {
          return findRealImpl( delegate );
        }
      }
    }
    return null;
  }

  @VisibleForTesting
  static Object getFieldValue( Field field, Object object ) {
    final boolean setAccessible = !field.isAccessible();
    try {
      if ( setAccessible ) {
        field.setAccessible( true );
      }
      return field.get( object );
    } catch ( IllegalAccessException e ) {
      // Shouldn't happen
    } finally {
      if ( setAccessible ) {
        field.setAccessible( false );
      }
    }
    return null;
  }

  @VisibleForTesting
  static HBaseConnection unwrapProxy( Object proxy ) {
    InvocationHandler invocationHandler = Proxy.getInvocationHandler( proxy );
    Class<?> clazz = invocationHandler.getClass();
    while ( clazz != null ) {
      for ( Field field : clazz.getDeclaredFields() ) {
        Object value = getFieldValue( field, invocationHandler );
        if ( value instanceof HBaseConnection ) {
          return findRealImpl( value );
        }
      }
      clazz = clazz.getSuperclass();
    }
    return null;
  }

  @VisibleForTesting
  Field getResultSetRowField() {
    return resultSetRowField;
  }

  @VisibleForTesting
  HBaseConnection getRealImpl() {
    return realImpl;
  }

  @Override public List<String> listNamespaces() throws Exception {
    return delegate.listNamespaces();
  }

  @Override public List<String> listTableNamesByNamespace( String namespace ) throws Exception {
    return delegate.listTableNamesByNamespace( namespace );
  }
}

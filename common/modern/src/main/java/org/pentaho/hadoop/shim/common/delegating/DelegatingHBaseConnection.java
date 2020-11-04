/*******************************************************************************
*
* Pentaho Big Data
*
* Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
*
*******************************************************************************
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
******************************************************************************/

package org.pentaho.hadoop.shim.common.delegating;

import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hbase.shim.api.ColumnFilter;
import org.pentaho.hbase.shim.api.HBaseValueMeta;
import org.pentaho.hbase.shim.common.wrapper.HBaseConnectionInterface;
import org.pentaho.hbase.shim.spi.HBaseBytesUtilShim;
import org.pentaho.hbase.shim.spi.HBaseConnection;

import java.util.List;
import java.util.NavigableMap;
import java.util.Properties;

public class DelegatingHBaseConnection extends HBaseConnection implements HBaseConnectionInterface {
  private final HBaseConnectionInterface delegate;

  public DelegatingHBaseConnection( HBaseConnectionInterface delegate ) {
    this.delegate = delegate;
  }

  @Override
  public void addColumnFilterToScan( ColumnFilter cf, HBaseValueMeta columnMeta, VariableSpace vars, boolean matchAny )
    throws Exception {
    delegate.addColumnFilterToScan( cf, columnMeta, vars, matchAny );
  }

  @Override
  public void addColumnToScan( String colFamilyName, String colName, boolean colNameIsBinary ) throws Exception {
    delegate.addColumnToScan( colFamilyName, colName, colNameIsBinary );
  }

  @Override
  public void addColumnToTargetPut( String columnFamily, String columnName, boolean colNameIsBinary, byte[] colValue )
    throws Exception {
    delegate.addColumnToTargetPut( columnFamily, columnName, colNameIsBinary, colValue );
  }

  @Override
  public boolean checkForHBaseRow( Object rowToCheck ) {
    return delegate.checkForHBaseRow( rowToCheck );
  }

  @Override
  public void checkHBaseAvailable() throws Exception {
    delegate.checkHBaseAvailable();
  }

  @Override
  public void closeSourceResultSet() throws Exception {
    delegate.closeSourceResultSet();
  }

  @Override
  public void closeSourceTable() throws Exception {
    delegate.closeSourceTable();
  }

  @Override
  public void closeTargetTable() throws Exception {
    delegate.closeTargetTable();
  }

  @Override
  public void configureConnection( Properties connProps, List<String> logMessages ) throws Exception {
    delegate.configureConnection( connProps, logMessages );
  }

  @Override
  public void createTable( String tableName, List<String> colFamilyNames, Properties creationProps ) throws Exception {
    delegate.createTable( tableName, colFamilyNames, creationProps );
  }

  @Override
  public void deleteTable( String tableName ) throws Exception {
    delegate.deleteTable( tableName );
  }

  @Override
  public void disableTable( String tableName ) throws Exception {
    delegate.disableTable( tableName );
  }

  @Override
  public void enableTable( String tableName ) throws Exception {
    delegate.enableTable( tableName );
  }

  @Override
  public boolean equals( Object obj ) {
    return delegate.equals( obj );
  }

  @Override
  public void executeSourceTableScan() throws Exception {
    delegate.executeSourceTableScan();
  }

  @Override
  public void executeTargetTableDelete( byte[] rowKey ) throws Exception {
    delegate.executeTargetTableDelete( rowKey );
  }

  @Override
  public void executeTargetTablePut() throws Exception {
    delegate.executeTargetTablePut();
  }

  @Override
  public void flushCommitsTargetTable() throws Exception {
    delegate.flushCommitsTargetTable();
  }

  @Override
  public Class<?> getBloomTypeClass() throws ClassNotFoundException {
    return delegate.getBloomTypeClass();
  }

  @Override
  public Class<?> getByteArrayComparableClass() throws ClassNotFoundException {
    return delegate.getByteArrayComparableClass();
  }

  @Override
  public HBaseBytesUtilShim getBytesUtil() throws Exception {
    return delegate.getBytesUtil();
  }

  @Override
  public Class<?> getCompressionAlgorithmClass() throws ClassNotFoundException {
    return delegate.getCompressionAlgorithmClass();
  }

  @Override
  public Class<?> getDeserializedBooleanComparatorClass() throws ClassNotFoundException {
    return delegate.getDeserializedBooleanComparatorClass();
  }

  @Override
  public Class<?> getDeserializedNumericComparatorClass() throws ClassNotFoundException {
    return delegate.getDeserializedNumericComparatorClass();
  }

  @Override
  public byte[] getResultSetCurrentRowColumnLatest( String colFamilyName, String colName, boolean colNameIsBinary )
    throws Exception {
    return delegate.getResultSetCurrentRowColumnLatest( colFamilyName, colName, colNameIsBinary );
  }

  @Override
  public NavigableMap<byte[], byte[]> getResultSetCurrentRowFamilyMap( String familyName ) throws Exception {
    return delegate.getResultSetCurrentRowFamilyMap( familyName );
  }

  @Override
  public byte[] getResultSetCurrentRowKey() throws Exception {
    return delegate.getResultSetCurrentRowKey();
  }

  @Override
  public NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> getResultSetCurrentRowMap()
    throws Exception {
    return delegate.getResultSetCurrentRowMap();
  }

  @Override
  public byte[] getRowColumnLatest( Object aRow, String colFamilyName, String colName, boolean colNameIsBinary )
    throws Exception {
    return delegate.getRowColumnLatest( aRow, colFamilyName, colName, colNameIsBinary );
  }

  @Override
  public NavigableMap<byte[], byte[]> getRowFamilyMap( Object aRow, String familyName ) throws Exception {
    return delegate.getRowFamilyMap( aRow, familyName );
  }

  @Override
  public byte[] getRowKey( Object aRow ) throws Exception {
    return delegate.getRowKey( aRow );
  }

  @Override
  public NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> getRowMap( Object aRow )
    throws Exception {
    return delegate.getRowMap( aRow );
  }

  @Override
  public List<String> getTableFamiles( String tableName ) throws Exception {
    return delegate.getTableFamiles( tableName );
  }

  @Override
  public int hashCode() {
    return delegate.hashCode();
  }

  @Override
  public boolean isImmutableBytesWritable( Object o ) {
    return delegate.isImmutableBytesWritable( o );
  }

  @Override
  public boolean isTableAvailable( String tableName ) throws Exception {
    return delegate.isTableAvailable( tableName );
  }

  @Override
  public boolean isTableDisabled( String tableName ) throws Exception {
    return delegate.isTableDisabled( tableName );
  }

  @Override
  public List<String> listTableNames() throws Exception {
    return delegate.listTableNames();
  }

  @Override
  public void newSourceTable( String tableName ) throws Exception {
    delegate.newSourceTable( tableName );
  }

  @Override
  public void newSourceTableScan( byte[] keyLowerBound, byte[] keyUpperBound, int cacheSize ) throws Exception {
    delegate.newSourceTableScan( keyLowerBound, keyUpperBound, cacheSize );
  }

  @Override
  public void newTargetTable( String tableName, Properties props ) throws Exception {
    delegate.newTargetTable( tableName, props );
  }

  @Override
  public void newTargetTablePut( byte[] key, boolean writeToWAL ) throws Exception {
    delegate.newTargetTablePut( key, writeToWAL );
  }

  @Override
  public boolean resultSetNextRow() throws Exception {
    return delegate.resultSetNextRow();
  }

  @Override
  public boolean sourceTableRowExists( byte[] rowKey ) throws Exception {
    return delegate.sourceTableRowExists( rowKey );
  }

  @Override
  public boolean tableExists( String tableName ) throws Exception {
    return delegate.tableExists( tableName );
  }

  @Override
  public boolean targetTableIsAutoFlush() throws Exception {
    return delegate.targetTableIsAutoFlush();
  }

  @Override
  public String toString() {
    return delegate.toString();
  }

  @Override
  public void close() throws Exception {
    delegate.close();
  }

  @Override
  public void obtainAuthTokenForJob( Configuration conf ) throws Exception {
    delegate.obtainAuthTokenForJob( conf );
  }
}

/*******************************************************************************
*
* Pentaho Big Data
*
* Copyright (C) 2002-2014 by Pentaho : http://www.pentaho.com
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

package org.pentaho.hadoop.shim.mapr31.delegatingShims;

import java.util.List;
import java.util.NavigableMap;
import java.util.Properties;

import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.hbase.shim.api.ColumnFilter;
import org.pentaho.hbase.shim.api.HBaseValueMeta;
import org.pentaho.hbase.shim.mapr31.wrapper.HBaseConnectionInterface;
import org.pentaho.hbase.shim.spi.HBaseBytesUtilShim;
import org.pentaho.hbase.shim.spi.HBaseConnection;

public class DelegatingHBaseConnection extends HBaseConnection implements HBaseConnectionInterface {
  private final HBaseConnectionInterface delegate;

  public DelegatingHBaseConnection( HBaseConnectionInterface delegate ) {
    this.delegate = delegate;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#addColumnFilterToScan(org.pentaho.hbase
   * .shim.api.ColumnFilter, org.pentaho.hbase.shim.api.HBaseValueMeta, org.pentaho.di.core.variables.VariableSpace,
   * boolean)
   */
  @Override
  public void addColumnFilterToScan( ColumnFilter cf, HBaseValueMeta columnMeta, VariableSpace vars, boolean matchAny )
    throws Exception {
    delegate.addColumnFilterToScan( cf, columnMeta, vars, matchAny );
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#addColumnToScan(java.lang.String,
   * java.lang.String, boolean)
   */
  @Override
  public void addColumnToScan( String colFamilyName, String colName, boolean colNameIsBinary ) throws Exception {
    delegate.addColumnToScan( colFamilyName, colName, colNameIsBinary );
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#addColumnToTargetPut(java.lang.String,
   * java.lang.String, boolean, byte[])
   */
  @Override
  public void addColumnToTargetPut( String columnFamily, String columnName, boolean colNameIsBinary, byte[] colValue )
    throws Exception {
    delegate.addColumnToTargetPut( columnFamily, columnName, colNameIsBinary, colValue );
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#checkForHBaseRow(java.lang.Object)
   */
  @Override
  public boolean checkForHBaseRow( Object rowToCheck ) {
    return delegate.checkForHBaseRow( rowToCheck );
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#checkHBaseAvailable()
   */
  @Override
  public void checkHBaseAvailable() throws Exception {
    delegate.checkHBaseAvailable();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#closeSourceResultSet()
   */
  @Override
  public void closeSourceResultSet() throws Exception {
    delegate.closeSourceResultSet();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#closeSourceTable()
   */
  @Override
  public void closeSourceTable() throws Exception {
    delegate.closeSourceTable();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#closeTargetTable()
   */
  @Override
  public void closeTargetTable() throws Exception {
    delegate.closeTargetTable();
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#configureConnection(java.util.Properties,
   * java.util.List)
   */
  @Override
  public void configureConnection( Properties connProps, List<String> logMessages ) throws Exception {
    delegate.configureConnection( connProps, logMessages );
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#createTable(java.lang.String,
   * java.util.List, java.util.Properties)
   */
  @Override
  public void createTable( String tableName, List<String> colFamilyNames, Properties creationProps ) throws Exception {
    delegate.createTable( tableName, colFamilyNames, creationProps );
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#deleteTable(java.lang.String)
   */
  @Override
  public void deleteTable( String tableName ) throws Exception {
    delegate.deleteTable( tableName );
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#disableTable(java.lang.String)
   */
  @Override
  public void disableTable( String tableName ) throws Exception {
    delegate.disableTable( tableName );
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#enableTable(java.lang.String)
   */
  @Override
  public void enableTable( String tableName ) throws Exception {
    delegate.enableTable( tableName );
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#equals(java.lang.Object)
   */
  @Override
  public boolean equals( Object obj ) {
    return delegate.equals( obj );
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#executeSourceTableScan()
   */
  @Override
  public void executeSourceTableScan() throws Exception {
    delegate.executeSourceTableScan();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#executeTargetTableDelete(byte[])
   */
  @Override
  public void executeTargetTableDelete( byte[] rowKey ) throws Exception {
    delegate.executeTargetTableDelete( rowKey );
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#executeTargetTablePut()
   */
  @Override
  public void executeTargetTablePut() throws Exception {
    delegate.executeTargetTablePut();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#flushCommitsTargetTable()
   */
  @Override
  public void flushCommitsTargetTable() throws Exception {
    delegate.flushCommitsTargetTable();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#getBloomTypeClass()
   */
  @Override
  public Class<?> getBloomTypeClass() throws ClassNotFoundException {
    return delegate.getBloomTypeClass();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#getByteArrayComparableClass()
   */
  @Override
  public Class<?> getByteArrayComparableClass() throws ClassNotFoundException {
    return delegate.getByteArrayComparableClass();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#getBytesUtil()
   */
  @Override
  public HBaseBytesUtilShim getBytesUtil() throws Exception {
    return delegate.getBytesUtil();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#getCompressionAlgorithmClass()
   */
  @Override
  public Class<?> getCompressionAlgorithmClass() throws ClassNotFoundException {
    return delegate.getCompressionAlgorithmClass();
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#getDeserializedBooleanComparatorClass()
   */
  @Override
  public Class<?> getDeserializedBooleanComparatorClass() throws ClassNotFoundException {
    return delegate.getDeserializedBooleanComparatorClass();
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#getDeserializedNumericComparatorClass()
   */
  @Override
  public Class<?> getDeserializedNumericComparatorClass() throws ClassNotFoundException {
    return delegate.getDeserializedNumericComparatorClass();
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#getResultSetCurrentRowColumnLatest(java
   * .lang.String, java.lang.String, boolean)
   */
  @Override
  public byte[] getResultSetCurrentRowColumnLatest( String colFamilyName, String colName, boolean colNameIsBinary )
    throws Exception {
    return delegate.getResultSetCurrentRowColumnLatest( colFamilyName, colName, colNameIsBinary );
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#getResultSetCurrentRowFamilyMap(java.lang
   * .String)
   */
  @Override
  public NavigableMap<byte[], byte[]> getResultSetCurrentRowFamilyMap( String familyName ) throws Exception {
    return delegate.getResultSetCurrentRowFamilyMap( familyName );
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#getResultSetCurrentRowKey()
   */
  @Override
  public byte[] getResultSetCurrentRowKey() throws Exception {
    return delegate.getResultSetCurrentRowKey();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#getResultSetCurrentRowMap()
   */
  @Override
  public NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> getResultSetCurrentRowMap()
    throws Exception {
    return delegate.getResultSetCurrentRowMap();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#getRowColumnLatest(java.lang.Object,
   * java.lang.String, java.lang.String, boolean)
   */
  @Override
  public byte[] getRowColumnLatest( Object aRow, String colFamilyName, String colName, boolean colNameIsBinary )
    throws Exception {
    return delegate.getRowColumnLatest( aRow, colFamilyName, colName, colNameIsBinary );
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#getRowFamilyMap(java.lang.Object,
   * java.lang.String)
   */
  @Override
  public NavigableMap<byte[], byte[]> getRowFamilyMap( Object aRow, String familyName ) throws Exception {
    return delegate.getRowFamilyMap( aRow, familyName );
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#getRowKey(java.lang.Object)
   */
  @Override
  public byte[] getRowKey( Object aRow ) throws Exception {
    return delegate.getRowKey( aRow );
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#getRowMap(java.lang.Object)
   */
  @Override
  public NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> getRowMap( Object aRow )
    throws Exception {
    return delegate.getRowMap( aRow );
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#getTableFamiles(java.lang.String)
   */
  @Override
  public List<String> getTableFamiles( String tableName ) throws Exception {
    return delegate.getTableFamiles( tableName );
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#hashCode()
   */
  @Override
  public int hashCode() {
    return delegate.hashCode();
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#isImmutableBytesWritable(java.lang.Object)
   */
  @Override
  public boolean isImmutableBytesWritable( Object o ) {
    return delegate.isImmutableBytesWritable( o );
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#isTableAvailable(java.lang.String)
   */
  @Override
  public boolean isTableAvailable( String tableName ) throws Exception {
    return delegate.isTableAvailable( tableName );
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#isTableDisabled(java.lang.String)
   */
  @Override
  public boolean isTableDisabled( String tableName ) throws Exception {
    return delegate.isTableDisabled( tableName );
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#listTableNames()
   */
  @Override
  public List<String> listTableNames() throws Exception {
    return delegate.listTableNames();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#newSourceTable(java.lang.String)
   */
  @Override
  public void newSourceTable( String tableName ) throws Exception {
    delegate.newSourceTable( tableName );
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#newSourceTableScan(byte[], byte[],
   * int)
   */
  @Override
  public void newSourceTableScan( byte[] keyLowerBound, byte[] keyUpperBound, int cacheSize ) throws Exception {
    delegate.newSourceTableScan( keyLowerBound, keyUpperBound, cacheSize );
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#newTargetTable(java.lang.String,
   * java.util.Properties)
   */
  @Override
  public void newTargetTable( String tableName, Properties props ) throws Exception {
    delegate.newTargetTable( tableName, props );
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#newTargetTablePut(byte[], boolean)
   */
  @Override
  public void newTargetTablePut( byte[] key, boolean writeToWAL ) throws Exception {
    delegate.newTargetTablePut( key, writeToWAL );
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#resultSetNextRow()
   */
  @Override
  public boolean resultSetNextRow() throws Exception {
    return delegate.resultSetNextRow();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#sourceTableRowExists(byte[])
   */
  @Override
  public boolean sourceTableRowExists( byte[] rowKey ) throws Exception {
    return delegate.sourceTableRowExists( rowKey );
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#tableExists(java.lang.String)
   */
  @Override
  public boolean tableExists( String tableName ) throws Exception {
    return delegate.tableExists( tableName );
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hadoop.shim.mapr31.delegatingShims.HBaseConnectionInterface#targetTableIsAutoFlush()
   */
  @Override
  public boolean targetTableIsAutoFlush() throws Exception {
    return delegate.targetTableIsAutoFlush();
  }

  @Override
  public String toString() {
    return delegate.toString();
  }
}

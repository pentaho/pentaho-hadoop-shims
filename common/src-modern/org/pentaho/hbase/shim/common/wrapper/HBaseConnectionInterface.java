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

package org.pentaho.hbase.shim.common.wrapper;

import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hbase.shim.api.ColumnFilter;
import org.pentaho.hbase.shim.api.HBaseValueMeta;
import org.pentaho.hbase.shim.spi.HBaseBytesUtilShim;

import java.util.List;
import java.util.NavigableMap;
import java.util.Properties;

public interface HBaseConnectionInterface {

  public abstract void addColumnFilterToScan( ColumnFilter cf, HBaseValueMeta columnMeta, VariableSpace vars,
                                              boolean matchAny ) throws Exception;

  public abstract void addColumnToScan( String colFamilyName, String colName, boolean colNameIsBinary )
    throws Exception;

  public abstract void addColumnToTargetPut( String columnFamily, String columnName, boolean colNameIsBinary,
                                             byte[] colValue ) throws Exception;

  public abstract boolean checkForHBaseRow( Object rowToCheck );

  public abstract void checkHBaseAvailable() throws Exception;

  public abstract void closeSourceResultSet() throws Exception;

  public abstract void closeSourceTable() throws Exception;

  public abstract void closeTargetTable() throws Exception;

  public abstract void configureConnection( Properties connProps, List<String> logMessages ) throws Exception;

  public abstract void createTable( String tableName, List<String> colFamilyNames, Properties creationProps )
    throws Exception;

  public abstract void deleteTable( String tableName ) throws Exception;

  public abstract void disableTable( String tableName ) throws Exception;

  public abstract void enableTable( String tableName ) throws Exception;

  public abstract void executeSourceTableScan() throws Exception;

  public abstract void executeTargetTableDelete( byte[] rowKey ) throws Exception;

  public abstract void executeTargetTablePut() throws Exception;

  public abstract void flushCommitsTargetTable() throws Exception;

  public abstract Class<?> getBloomTypeClass() throws ClassNotFoundException;

  public abstract Class<?> getByteArrayComparableClass() throws ClassNotFoundException;

  public abstract HBaseBytesUtilShim getBytesUtil() throws Exception;

  public abstract Class<?> getCompressionAlgorithmClass() throws ClassNotFoundException;

  public abstract Class<?> getDeserializedBooleanComparatorClass() throws ClassNotFoundException;

  public abstract Class<?> getDeserializedNumericComparatorClass() throws ClassNotFoundException;

  public abstract byte[] getResultSetCurrentRowColumnLatest( String colFamilyName, String colName,
                                                             boolean colNameIsBinary ) throws Exception;

  public abstract NavigableMap<byte[], byte[]> getResultSetCurrentRowFamilyMap( String familyName ) throws Exception;

  public abstract byte[] getResultSetCurrentRowKey() throws Exception;

  public abstract NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> getResultSetCurrentRowMap()
    throws Exception;

  public abstract byte[]
  getRowColumnLatest( Object aRow, String colFamilyName, String colName, boolean colNameIsBinary ) throws Exception;

  public abstract NavigableMap<byte[], byte[]> getRowFamilyMap( Object aRow, String familyName ) throws Exception;

  public abstract byte[] getRowKey( Object aRow ) throws Exception;

  public abstract NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> getRowMap( Object aRow )
    throws Exception;

  public abstract List<String> getTableFamiles( String tableName ) throws Exception;

  public abstract boolean isImmutableBytesWritable( Object o );

  public abstract boolean isTableAvailable( String tableName ) throws Exception;

  public abstract boolean isTableDisabled( String tableName ) throws Exception;

  public abstract List<String> listTableNames() throws Exception;

  public abstract void newSourceTable( String tableName ) throws Exception;

  public abstract void newSourceTableScan( byte[] keyLowerBound, byte[] keyUpperBound, int cacheSize ) throws Exception;

  public abstract void newTargetTable( String tableName, Properties props ) throws Exception;

  public abstract void newTargetTablePut( byte[] key, boolean writeToWAL ) throws Exception;

  public abstract boolean resultSetNextRow() throws Exception;

  public abstract boolean sourceTableRowExists( byte[] rowKey ) throws Exception;

  public abstract boolean tableExists( String tableName ) throws Exception;

  public abstract boolean targetTableIsAutoFlush() throws Exception;

  public abstract void close() throws Exception;

  public abstract void obtainAuthTokenForJob( Configuration conf ) throws Exception;

}

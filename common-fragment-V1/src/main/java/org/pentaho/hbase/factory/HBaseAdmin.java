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
package org.pentaho.hbase.factory;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HTableDescriptor;

@SuppressWarnings( "squid:S112" )
public interface HBaseAdmin {
  HTableDescriptor[] listTables() throws IOException;

  boolean tableExists( String tableName ) throws IOException;

  void disableTable( String tableName ) throws IOException;

  void enableTable( String tableName ) throws IOException;

  boolean isTableDisabled( String tableName ) throws IOException;

  boolean isTableEnabled( String tableName ) throws IOException;

  boolean isTableAvailable( String tableName ) throws IOException;

  HTableDescriptor getTableDescriptor( byte[] tableName ) throws IOException;

  void deleteTable( String tableName ) throws IOException;

  void createTable( HTableDescriptor tableDesc ) throws IOException;

  void close() throws IOException;

  default List<String> listNamespaces() throws Exception {
    throw new UnsupportedOperationException( "This method has not supported with the present HbaseConnection" );
  }

  default List<String> listTableNamesByNamespace( String namespace ) throws Exception {
    throw new UnsupportedOperationException( "This method has not supported with the present HbaseConnection" );
  }
}

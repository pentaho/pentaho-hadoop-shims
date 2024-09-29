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

package org.pentaho.hadoop.hbase.factory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.pentaho.hbase.factory.HBaseAdmin;

class HBase10Admin implements HBaseAdmin {
  private final Admin admin;

  HBase10Admin( Connection conn ) throws IOException {
    admin = conn.getAdmin();
  }

  @Override
  public boolean tableExists( String tableName ) throws IOException {
    return admin.tableExists( TableName.valueOf( tableName ) );
  }

  @Override
  public HTableDescriptor[] listTables() throws IOException {
    return admin.listTables();
  }

  @Override
  public boolean isTableDisabled( String tableName ) throws IOException {
    return admin.isTableDisabled( TableName.valueOf( tableName ) );
  }

  @Override
  public boolean isTableEnabled( String tableName ) throws IOException {
    return admin.isTableEnabled( TableName.valueOf( tableName ) );
  }

  @Override
  public boolean isTableAvailable( String tableName ) throws IOException {
    return admin.isTableAvailable( TableName.valueOf( tableName ) );
  }

  @Override
  public HTableDescriptor getTableDescriptor( byte[] tableName ) throws IOException {
    return admin.getTableDescriptor( TableName.valueOf( tableName ) );
  }

  @Override
  public void enableTable( String tableName ) throws IOException {
    admin.enableTable( TableName.valueOf( tableName ) );
  }

  @Override
  public void disableTable( String tableName ) throws IOException {
    admin.disableTable( TableName.valueOf( tableName ) );
  }

  @Override
  public void deleteTable( String tableName ) throws IOException {
    admin.deleteTable( TableName.valueOf( tableName ) );
  }

  @Override
  public void createTable( HTableDescriptor tableDesc ) throws IOException {

    boolean useInterface = false;
    String tableDescriptorInterface = "org.apache.hadoop.hbase.client.TableDescriptor";
    for ( Class descInterface : tableDesc.getClass().getInterfaces() ) {
      if ( descInterface.getName().equals( tableDescriptorInterface ) ) {
        useInterface = true;
        break;
      }
    }
    try {
      if ( useInterface ) {
        admin.getClass().getMethod( "createTable", Class.forName( tableDescriptorInterface ) )
          .invoke( admin, tableDesc );
      } else {
        admin.getClass().getMethod( "createTable", HTableDescriptor.class ).invoke( admin, tableDesc );
      }
    } catch ( Exception ex ) {
      throw new IOException( ex );
    }
  }

  @Override
  public void close() throws IOException {
    admin.close();
  }

  @Override public List<String> listNamespaces() throws Exception {
    NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
    return Stream.of( namespaceDescriptors ).map( NamespaceDescriptor::getName ).collect( Collectors.toList() );
  }

  @Override public List<String> listTableNamesByNamespace( String namespace ) throws Exception {
    TableName[] tableNames = admin.listTableNamesByNamespace( namespace );
    return Stream.of( tableNames ).map( TableName::getNameAsString ).collect( Collectors.toList() );
  }
}

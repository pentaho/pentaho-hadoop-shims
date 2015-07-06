/*******************************************************************************
*
* Pentaho Big Data
*
* Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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
package org.pentaho.hadoop.hbase.factory;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.pentaho.hbase.factory.HBaseAdmin;

class HBase9xAdmin implements HBaseAdmin {
  private final org.apache.hadoop.hbase.client.HBaseAdmin admin;

  HBase9xAdmin( Configuration conf ) throws IOException {
    admin = new org.apache.hadoop.hbase.client.HBaseAdmin(conf);
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
  public HTableDescriptor getTableDescriptor( String tableName ) throws IOException {
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
    admin.createTable( tableDesc );
  }

  @Override
  public void close() throws IOException {
    admin.close();
  }
}

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
package org.pentaho.hbase.factory;

import java.io.IOException;

import org.apache.hadoop.hbase.HTableDescriptor;

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
}

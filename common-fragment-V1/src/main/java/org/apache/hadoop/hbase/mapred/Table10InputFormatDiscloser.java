/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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
package org.apache.hadoop.hbase.mapred;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.pentaho.hbase.mapred.PentahoTableInputFormat;

public class Table10InputFormatDiscloser extends TableInputFormatBase {

  private PentahoTableInputFormat common;

  public Table10InputFormatDiscloser( PentahoTableInputFormat common ) {
    this.common = common;
  }

  @Override
  public void initializeTable( Connection connection, TableName tableName ) throws IOException {
    common.initializeTable( connection, tableName );
  }

  @Override
  public Table getTable() {
    return common.getTable();
  }

}

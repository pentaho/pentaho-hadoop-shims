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

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
package org.pentaho.hadoop.hbase.factory;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapred.Table10InputFormatDiscloser;
import org.pentaho.hbase.factory.HBaseAdmin;
import org.pentaho.hbase.factory.HBaseClientFactory;
import org.pentaho.hbase.factory.HBaseClientFactoryLocator;
import org.pentaho.hbase.factory.HBasePut;
import org.pentaho.hbase.factory.HBaseTable;
import org.pentaho.hbase.mapred.PentahoTableInputFormat;
import org.pentaho.hbase.mapred.PentahoTableRecordReader;

public class HBase10ClientFactory implements HBaseClientFactory {
  private Connection conn = null;
  private final Configuration conf;

  public HBase10ClientFactory( Configuration conf ) throws Exception {
    this.conf = conf;
    if ( conf != null ) {
      conn = ConnectionFactory.createConnection( conf );
    } else {
      conn = null;
    }
  }

  public synchronized Connection getConnection() throws IOException {
    if(conn == null) {
      conn = ConnectionFactory.createConnection( conf );
    }
    
    return conn;
  }

  @Override
  public HBaseTable getHBaseTable( final String tableName ) {
    try {
      return new HBase10Table( getConnection(), tableName );
    } catch ( IOException e ) {
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public HBaseAdmin getHBaseAdmin() {
    try {
      return new HBase10Admin( getConnection() );
    } catch ( IOException e ) {
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public void close() {
    try {
      conn.close();
    } catch ( IOException e ) {
      e.printStackTrace();
    }
  }

  @Override
  public HTableDescriptor getHBaseTableDescriptor( String tableName ) {
    return new HTableDescriptor( TableName.valueOf( tableName ) );
  }

  @Override
  public HBaseTable wrap( Object tableObject ) {
    if ( tableObject == null ) {
      throw new NullPointerException( "null as a table was passed" );
    }

    Table tab = null;
    if( tableObject instanceof Table ) {
      tab = ( Table ) tableObject;
      return new HBase10Table( tab );
    } 

    throw new IllegalArgumentException( "Type mismatch:" + Table.class.getCanonicalName() + " was expected" );
  }

  @Override
  public PentahoTableInputFormat getTableInputFormatImpl(
      final PentahoTableInputFormat common, 
      final Configuration conf ) {
    return new PentahoTableInputFormat() {
      Table10InputFormatDiscloser invoker = new Table10InputFormatDiscloser( common );

      @Override
      protected void setHBaseTable( Configuration conf, String tableName ) throws IOException {
        final Connection conn = ConnectionFactory.createConnection( conf );
        invoker.initializeTable( conn, TableName.valueOf( tableName ) );
      }

      @Override
      protected boolean checkHBaseTable() {
        return invoker.getTable() != null;
      }

      @Override
      protected PentahoTableRecordReader createRecordReader( final Configuration config ) {
        return new PentahoTableRecordReader () {
          @Override
          public void setHTable( Table table ) {
            HBaseClientFactory hbcf = HBaseClientFactoryLocator.getHBaseClientFactory( config );
            getImpl().setHTable( hbcf.wrap ( table ) );
          }
        };
      }

    };
  }

  @Override
  public HBasePut getHBasePut( byte[] key ) {
    return new HBase10Put( key );
  }

}
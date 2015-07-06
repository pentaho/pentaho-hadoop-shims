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
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapred.Table9xInputFormatDiscloser;
import org.pentaho.hbase.factory.HBaseAdmin;
import org.pentaho.hbase.factory.HBaseClientFactory;
import org.pentaho.hbase.factory.HBaseClientFactoryLocator;
import org.pentaho.hbase.factory.HBasePut;
import org.pentaho.hbase.factory.HBaseTable;
import org.pentaho.hbase.mapred.PentahoTableInputFormat;
import org.pentaho.hbase.mapred.PentahoTableRecordReader;

class HBase9xClientFactory implements HBaseClientFactory {
  private final Configuration conf;

  HBase9xClientFactory( Configuration conf ) throws Exception {
    this.conf = conf;
  }

  @Override
  public HBaseTable getHBaseTable( final String tableName ) {
    try {
      return new HBase9xTable(conf, tableName);
    } catch ( IOException e ) {
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public HBaseAdmin getHBaseAdmin() {
    try {
      return new HBase9xAdmin( conf );
    } catch ( IOException e ) {
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public void close() {
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

    HTable tab = null;
    if ( tableObject instanceof HTable ) {
      tab = ( HTable ) tableObject;
      return new HBase9xTable( tab );
    } 

    throw new IllegalArgumentException( "Type mismatch:" + HTable.class.getCanonicalName() + " was expected" );
  }

  @Override
  public PentahoTableInputFormat getTableInputFormatImpl(
      final PentahoTableInputFormat common,
      final Configuration conf ) {
    return new PentahoTableInputFormat() {
      Table9xInputFormatDiscloser invoker = new Table9xInputFormatDiscloser( common );

      @Override
      protected void setHBaseTable( Configuration conf, String tableName ) throws IOException {
        invoker.setHTable( new HTable( conf, TableName.valueOf( tableName ) ) );
      }

      @Override
      protected boolean checkHBaseTable() {
        return invoker.getHTable() != null;
      }

      @Override
      protected PentahoTableRecordReader createRecordReader( final Configuration config ) {
        return new PentahoTableRecordReader () {
          @Override
          public void setHTable(HTable table) {
            HBaseClientFactory hbcf = HBaseClientFactoryLocator.getHBaseClientFactory( config );
            getImpl().setHTable( hbcf.wrap ( table ) );
          }
        };
      }

    };
  }

  @Override
  public HBasePut getHBasePut( byte[] key ) {
    return new HBase9xPut( key );
  }

}

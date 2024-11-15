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

package org.pentaho.hadoop.hbase.factory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapred.Table10InputFormatDiscloser;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hbase.factory.HBaseAdmin;
import org.pentaho.hbase.factory.HBaseClientFactory;

import org.pentaho.hbase.factory.HBasePut;
import org.pentaho.hbase.factory.HBaseTable;
import org.pentaho.hbase.mapred.PentahoTableInputFormat;
import org.pentaho.hbase.mapred.PentahoTableRecordReader;

import java.io.IOException;

public class HBase10ClientFactory implements HBaseClientFactory {
  protected final Configuration conf;
  protected Connection conn = null;
  protected NamedCluster namedCluster;

  public HBase10ClientFactory( Configuration conf ) throws IOException {
    this.conf = conf;
    if ( conf != null ) {
      conn = ConnectionFactory.createConnection( conf );
    } else {
      conn = null;
    }
  }

  public synchronized Connection getConnection() throws IOException {
    if ( conn == null ) {
      conn = ConnectionFactory.createConnection( conf );
    }

    return conn;
  }

  @Override public HBaseTable getHBaseTable( final String tableName ) {
    try {
      return new HBase10Table( getConnection(), tableName );
    } catch ( IOException e ) {
      e.printStackTrace();
      return null;
    }
  }

  @Override public HBaseAdmin getHBaseAdmin() {
    try {
      return new HBase10Admin( getConnection() );
    } catch ( IOException e ) {
      e.printStackTrace();
      return null;
    }
  }

  @Override public void close() {
    try {
      conn.close();
    } catch ( IOException e ) {
      e.printStackTrace();
    }
  }

  @Override public HTableDescriptor getHBaseTableDescriptor( String tableName ) {
    return new HTableDescriptor( TableName.valueOf( tableName ) );
  }

  @Override public HBaseTable wrap( Object tableObject ) {
    if ( tableObject == null ) {
      throw new NullPointerException( "null as a table was passed" );
    }

    Table tab = null;
    if ( tableObject instanceof Table ) {
      tab = (Table) tableObject;
      return new HBase10Table( tab );
    }

    throw new IllegalArgumentException( "Type mismatch:" + Table.class.getCanonicalName() + " was expected" );
  }

  @Override public PentahoTableInputFormat getTableInputFormatImpl( final PentahoTableInputFormat common,
                                                                    final Configuration conf ) {
    return new PentahoTableInputFormat() {
      Table10InputFormatDiscloser invoker = new Table10InputFormatDiscloser( common );

      @Override protected void setHBaseTable( Configuration conf, String tableName ) throws IOException {
        final Connection conn = ConnectionFactory.createConnection( conf );
        invoker.initializeTable( conn, TableName.valueOf( tableName ) );
      }

      @Override protected boolean checkHBaseTable() {
        return invoker.getTable() != null;
      }

      // the exception being caught and logged here was previously caught and logged in a different class
      @SuppressWarnings( {"squid:S2259", "squid:S1148"} )
      @Override protected PentahoTableRecordReader createRecordReader( final Configuration config ) {
        return new PentahoTableRecordReader() {
          @Override public void setHTable( Table table ) {
            HBaseClientFactory hbcf = null;
            try {
              hbcf = new HBase10ClientFactory( config );
            } catch ( IOException e ) {
              e.printStackTrace();
            }
            getImpl().setHTable( hbcf.wrap( table ) );
          }
        };
      }

    };
  }

  @Override public HBasePut getHBasePut( byte[] key ) {
    return new HBase10Put( key );
  }

  @Override public NamedCluster getNamedCluster() {
    return namedCluster;
  }

  @Override public void setNamedCluster( NamedCluster namedCluster ) {
    this.namedCluster = namedCluster;
  }
}

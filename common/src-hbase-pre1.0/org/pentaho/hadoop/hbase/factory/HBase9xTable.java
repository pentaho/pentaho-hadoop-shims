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
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.HTable;
import org.pentaho.hbase.factory.HBasePut;
import org.pentaho.hbase.factory.HBaseTable;

class HBase9xTable implements HBaseTable {
  private final HTable tab;
  
  HBase9xTable( Configuration conf, String tableName ) throws IOException {
    tab = new HTable( conf, tableName );
  }

  /**
   * Constructs read-only HBaseTable
   * @param tab - HBase Table to wrap
   */
  HBase9xTable(HTable tab) {
    this.tab = tab;
  }

  @Override
  public void setWriteBufferSize( long bufferSize ) throws IOException {
    tab.setWriteBufferSize( bufferSize );
  }

  @Override
  public void setAutoFlush( boolean autoFlush ) throws IOException {
    tab.setAutoFlush( autoFlush ); 
  }

  @Override
  public boolean isAutoFlush() throws IOException {
    return tab.isAutoFlush();
  }

  @Override
  public ResultScanner getScanner( Scan s ) throws IOException {
    return tab.getScanner( s );
  }

  @Override
  public Result get( Get toGet ) throws IOException {
    return tab.get( toGet );
  }

  @Override
  public void flushCommits() throws IOException {
    tab.flushCommits();
  }

  @Override
  public void delete( Delete toDel ) throws IOException {
    tab.delete( toDel );
  }

  @Override
  public void close() throws IOException {
    tab.close();
  }

  @Override
  public void put( HBasePut put ) throws IOException {
    if ( put == null ) {
      throw new NullPointerException( "NULL Put passed" );
    }
    if ( put instanceof HBase9xPut ) {
      HBase9xPut p9x = ( HBase9xPut ) put;
      put( p9x.getPut() );
    } else {
      throw new IllegalArgumentException( "Unexpected backed HBasePut type passed:" + put.getClass() );
    }
  }

  void put( Put toPut ) throws IOException {
    tab.put( toPut );
  }

}

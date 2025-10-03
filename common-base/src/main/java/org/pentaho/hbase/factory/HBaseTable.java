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

package org.pentaho.hbase.factory;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public interface HBaseTable {
  Result get( Get toGet ) throws IOException;

  ResultScanner getScanner( Scan s ) throws IOException;

  void put( HBasePut put ) throws IOException;

  void close() throws IOException;

  void delete( Delete toDel ) throws IOException;

  void flushCommits() throws IOException;

  void setWriteBufferSize( long bufferSize ) throws IOException;

  boolean isAutoFlush() throws IOException;

  void setAutoFlush( boolean autoFlush ) throws IOException;
}

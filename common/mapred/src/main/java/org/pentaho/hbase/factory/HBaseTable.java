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

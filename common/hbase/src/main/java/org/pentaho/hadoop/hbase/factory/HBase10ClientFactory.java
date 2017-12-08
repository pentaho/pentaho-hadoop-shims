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
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.hbase.factory.HBaseAdmin;
import org.pentaho.hbase.factory.HBaseClientFactory;
import org.pentaho.hbase.factory.HBaseClientFactoryLocator;
import org.pentaho.hbase.factory.HBasePut;
import org.pentaho.hbase.factory.HBaseTable;
import org.pentaho.hbase.mapred.PentahoTableInputFormat;
import org.pentaho.hbase.mapred.PentahoTableRecordReader;

public class HBase10ClientFactory extends BaseHBaseClientFactory {
  public HBase10ClientFactory( Configuration conf ) throws Exception {
    super( conf );
  }

  @Override protected Connection createConnection( Configuration conf ) throws IOException  {
    return ConnectionFactory.createConnection( conf );
  }
}
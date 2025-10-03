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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hbase.mapred.PentahoTableInputFormat;

public interface HBaseClientFactory {
  HBaseTable getHBaseTable( String tableName );

  HBaseAdmin getHBaseAdmin();

  HTableDescriptor getHBaseTableDescriptor( String tableName );

  HBaseTable wrap( Object tableObject );

  PentahoTableInputFormat getTableInputFormatImpl(
    PentahoTableInputFormat pentahoTableInputFormatBase,
    Configuration conf );

  HBasePut getHBasePut( byte[] key );

  NamedCluster getNamedCluster();

  void setNamedCluster( NamedCluster namedCluster );

  void close();
}

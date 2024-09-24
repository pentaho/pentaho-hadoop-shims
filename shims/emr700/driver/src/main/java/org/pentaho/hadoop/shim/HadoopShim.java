/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package org.pentaho.hadoop.shim;

import com.google.common.collect.Lists;
import com.google.protobuf.Message;
import com.yammer.metrics.core.MetricsRegistry;
import io.netty.channel.Channel;
import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.htrace.Trace;
import org.apache.zookeeper.ZooKeeper;
import org.apache.hadoop.hbase.mapreduce.JobUtil;
import org.apache.hadoop.hbase.metrics.impl.FastLongHistogram;
import org.apache.hadoop.hbase.metrics.Snapshot;

import org.pentaho.hadoop.shim.common.HadoopShimImpl;

public class HadoopShim extends HadoopShimImpl {

  public HadoopShim() {
    super();
  }

  static {
    JDBC_DRIVER_MAP.put( "hive2", org.apache.hive.jdbc.HiveDriver.class );
  }

  public Class[] getHbaseDependencyClasses() {
    return new Class[] {
      HConstants.class, ClientProtos.class, Put.class, CompatibilityFactory.class,
      JobUtil.class, TableMapper.class, FastLongHistogram.class, Snapshot.class,
      ZooKeeper.class, Channel.class, Message.class, Lists.class, Trace.class, MetricsRegistry.class
    };
  }
}

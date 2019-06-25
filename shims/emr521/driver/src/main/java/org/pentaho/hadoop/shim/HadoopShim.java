/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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
    return new Class[]{
            HConstants.class, ClientProtos.class, Put.class, CompatibilityFactory.class,
            JobUtil.class, TableMapper.class, FastLongHistogram.class, Snapshot.class,
            ZooKeeper.class, Channel.class, Message.class, Lists.class, Trace.class, MetricsRegistry.class
    };
  }
}

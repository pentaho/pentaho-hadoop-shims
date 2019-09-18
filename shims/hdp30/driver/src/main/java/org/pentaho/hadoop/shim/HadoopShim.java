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


import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.mapreduce.JobUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.metrics.impl.FastLongHistogram;
import org.apache.hadoop.hbase.metrics.Snapshot;
import org.apache.zookeeper.ZooKeeper;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.htrace.core.Tracer;
import com.codahale.metrics.MetricRegistry;
import org.apache.commons.lang3.ArrayUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.Versioned;
import com.fasterxml.jackson.annotation.JsonView;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;

import org.pentaho.hadoop.shim.common.ConfigurationProxyV2;
import org.pentaho.hadoop.shim.common.HadoopShimImpl;

public class HadoopShim extends HadoopShimImpl {

  public HadoopShim() {
    super();
  }

  @Override
  public Class[] getHbaseDependencyClasses() {
    return new Class[] {
      HConstants.class, org.apache.hadoop.hbase.protobuf.generated.ClientProtos.class,
      org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.class, Put.class,
      RpcServer.class, CompatibilityFactory.class, JobUtil.class, TableMapper.class, FastLongHistogram.class,
      Snapshot.class, ZooKeeper.class, Channel.class, Message.class, UnsafeByteOperations.class, Lists.class,
      Tracer.class, MetricRegistry.class, ArrayUtils.class, ObjectMapper.class, Versioned.class, JsonView.class,
      ZKWatcher.class
    };
  }

  @Override
  public org.pentaho.hadoop.shim.api.internal.Configuration createConfiguration( String namedClusterConfigId ) {
    ConfigurationProxyV2 conf = (ConfigurationProxyV2) super.createConfiguration( namedClusterConfigId );
    conf.getJob().getConfiguration().setRestrictSystemProperties( false );
    return conf;
  }

}

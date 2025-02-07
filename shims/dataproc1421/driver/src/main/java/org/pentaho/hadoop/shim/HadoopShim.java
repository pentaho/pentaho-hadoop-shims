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


import java.io.IOException;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.mapreduce.JobUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.metrics.Snapshot;
import org.apache.hadoop.hbase.metrics.impl.FastLongHistogram;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.zookeeper.ZooKeeper;
import org.pentaho.hadoop.shim.api.internal.Configuration;
import org.pentaho.hadoop.shim.api.internal.mapred.RunningJob;
import org.pentaho.hadoop.shim.common.ConfigurationProxyV2;
import org.pentaho.hadoop.shim.common.HadoopShimImpl;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.annotation.JsonView;
import com.fasterxml.jackson.core.Versioned;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Message;

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
      MetricRegistry.class, ArrayUtils.class, ObjectMapper.class, Versioned.class, JsonView.class,
      ZKWatcher.class
    };
  }
}

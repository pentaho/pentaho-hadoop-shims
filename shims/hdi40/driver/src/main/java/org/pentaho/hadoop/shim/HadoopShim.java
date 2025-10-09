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

package org.pentaho.hadoop.shim;

import com.fasterxml.jackson.annotation.JsonView;
import com.fasterxml.jackson.core.Versioned;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Message;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.mapreduce.JobUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.metrics.Snapshot;
import org.apache.hadoop.hbase.metrics.impl.FastLongHistogram;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hbase.thirdparty.com.google.common.cache.CacheLoader;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.zookeeper.ZooKeeper;
import org.pentaho.hadoop.shim.api.core.ShimIdentifierInterface;
import org.pentaho.hadoop.shim.api.internal.ShimIdentifier;
import org.pentaho.hadoop.shim.common.HadoopShimImpl;

public class HadoopShim extends HadoopShimImpl {

  private static final String ID = "hdi40";
  private static final String VENDOR = "Azure HDI";
  private static final String VERSION = "4.0";
  private static final ShimIdentifierInterface.ShimType TYPE = ShimIdentifierInterface.ShimType.COMMUNITY;
  private static final ShimIdentifier SHIM_IDENTIFIER = new ShimIdentifier( ID, VENDOR, VERSION, TYPE );

  @Override
  public Class[] getHbaseDependencyClasses() {
    return new Class[]{
      HConstants.class, ClientProtos.class, ClientProtos.class, Put.class, RpcServer.class, CompatibilityFactory.class,
      JobUtil.class, TableMapper.class, FastLongHistogram.class, Snapshot.class,
      ZooKeeper.class, Channel.class, Message.class, UnsafeByteOperations.class, Lists.class,
      MetricRegistry.class, ArrayUtils.class, ObjectMapper.class, Versioned.class,
      JsonView.class, ZKWatcher.class, CacheLoader.class };
  }

  @Override
  public ShimIdentifier getShimIdentifier() {
    return SHIM_IDENTIFIER;
  }

}

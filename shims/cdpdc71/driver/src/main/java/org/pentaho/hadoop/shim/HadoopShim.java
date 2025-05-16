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
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.ImplicitContextKeyed;
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
import org.apache.hadoop.hbase.unsafe.HBasePlatformDependent;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.zookeeper.ZooKeeper;
import org.apache.hbase.thirdparty.com.google.common.cache.CacheLoader;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.hadoop.shim.api.core.ShimIdentifierInterface;
import org.pentaho.hadoop.shim.api.internal.ShimIdentifier;
import org.pentaho.hadoop.shim.common.HadoopShimImpl;

import java.util.Properties;

public class HadoopShim extends HadoopShimImpl {

  private static final String ID = "cdpdc71";
  private static final String VENDOR = "Cloudera";
  private static final String VERSION = "7.1";
  private static final ShimIdentifierInterface.ShimType TYPE = ShimIdentifierInterface.ShimType.COMMUNITY;
  private static final ShimIdentifier SHIM_IDENTIFIER = new ShimIdentifier( ID, VENDOR, VERSION, TYPE );

  public HadoopShim() {
    super();
  }

  protected void registerExtraDatabaseTypes( Properties configuration ) throws KettlePluginException {

    String impalaSimbaDriverName =
      configuration.getProperty( "impala.simba.driver", "com.cloudera.impala.jdbc41.Driver" );
    JDBC_POSSIBLE_DRIVER_MAP.put( "ImpalaSimba", impalaSimbaDriverName );
  }

  @Override
  public Class[] getHbaseDependencyClasses() {
    return new Class[] {
      HConstants.class, org.apache.hadoop.hbase.protobuf.generated.ClientProtos.class,
      org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.class, Put.class,
      RpcServer.class, CompatibilityFactory.class, JobUtil.class, TableMapper.class, FastLongHistogram.class,
      Snapshot.class, ZooKeeper.class, Channel.class, Message.class, UnsafeByteOperations.class, Lists.class,
      MetricRegistry.class, ArrayUtils.class, ObjectMapper.class, Versioned.class,
      JsonView.class, ZKWatcher.class, CacheLoader.class, HBasePlatformDependent.class, Span.class,
      ImplicitContextKeyed.class
    };
  }

  @Override
  public ShimIdentifier getShimIdentifier() {
    return SHIM_IDENTIFIER;
  }

}

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


import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.hadoop.shim.common.HadoopShimImpl;

import java.util.Properties;

public class HadoopShim extends HadoopShimImpl {

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
      String[] classNames = getHbaseDependencyClassesNames();

      try {
        Class[] classes = new Class[classNames.length];
        for (int i = 0; i < classNames.length; i++) {
          classes[i] = Class.forName(classNames[i]);
        }
        return classes;
      } catch (ClassNotFoundException e) {
        return null;
      }
    }

  public String[] getHbaseDependencyClassesNames() {
    return new String[] {

            "org.apache.hadoop.io.BytesWritable",

            "org.apache.hadoop.hbase.HConstants",
            "org.apache.hadoop.hbase.protobuf.generated.ClientProtos",
            "org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos",
            "org.apache.hadoop.hbase.client.Put",
            "org.apache.hadoop.hbase.ipc.RpcServer",
            "org.apache.hadoop.hbase.CompatibilityFactory",
            "org.apache.hadoop.hbase.mapreduce.JobUtil",
            "org.apache.hadoop.hbase.mapreduce.TableMapper",
            "org.apache.hadoop.hbase.metrics.impl.FastLongHistogram",
            "org.apache.hadoop.hbase.metrics.Snapshot",
            "org.apache.zookeeper.ZooKeeper",
            "org.apache.hbase.thirdparty.io.netty.channel.Channel",
            "com.google.protobuf.Message",
            "org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations",
            "org.apache.hbase.thirdparty.com.google.common.collect.Lists",
            "org.apache.hadoop.hbase.metrics.MetricRegistry",
            "org.apache.commons.lang3.ArrayUtils",
            "com.fasterxml.jackson.databind.ObjectMapper",
            "com.fasterxml.jackson.core.Versioned",
            "com.fasterxml.jackson.annotation.JsonView",
            "org.apache.hadoop.hbase.zookeeper.ZKWatcher",
            "org.apache.hbase.thirdparty.com.google.common.cache.CacheLoader",
            "org.apache.hadoop.hbase.unsafe.HBasePlatformDependent",
            "io.opentelemetry.api.trace.Span",
            "io.opentelemetry.context.ImplicitContextKeyed"
    };
  }

}

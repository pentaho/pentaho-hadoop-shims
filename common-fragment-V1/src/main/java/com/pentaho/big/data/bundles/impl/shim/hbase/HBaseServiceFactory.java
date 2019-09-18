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

package com.pentaho.big.data.bundles.impl.shim.hbase;

import org.pentaho.hadoop.shim.api.ConfigurationException;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceFactory;
import org.pentaho.hadoop.shim.api.hbase.HBaseService;
import org.pentaho.hadoop.shim.spi.HBaseShim;

/**
 * Created by bryan on 1/27/16.
 */
public class HBaseServiceFactory implements NamedClusterServiceFactory<HBaseService> {
  private final boolean isActiveConfiguration;
  private final HBaseShim hbaseShim;

  public HBaseServiceFactory( HBaseShim hbaseShim ) {
    this( true, hbaseShim );
  }

  public HBaseServiceFactory( boolean isActiveConfiguration, HBaseShim hbaseShim ) {
    this.isActiveConfiguration = isActiveConfiguration;
    this.hbaseShim = hbaseShim;
  }

  @Override public Class<HBaseService> getServiceClass() {
    return HBaseService.class;
  }

  @Override public boolean canHandle( NamedCluster namedCluster ) {
    //    if (namedCluster.getName().startsWith( "hdp" ) && hadoopConfiguration.getHadoopConfiguration().getName()
    //    .toLowerCase().contains( "hdp" )) {
    //      return true;
    //    }
    //    if (namedCluster.getName().startsWith( "cdh" ) && hadoopConfiguration.getHadoopConfiguration().getName()
    //    .toLowerCase().contains( "cdh" )) {
    //      return true;
    //    }
    return true;
    //return false;
  }

  @Override public HBaseService create( NamedCluster namedCluster ) {
    try {
      return new HBaseServiceImpl( namedCluster, hbaseShim );
    } catch ( ConfigurationException e ) {
      return null;
    }
  }
}

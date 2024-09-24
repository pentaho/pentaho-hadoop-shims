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

    if ( namedCluster.isUseGateway() ) {
      return false;
    }

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

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


package org.pentaho.big.data.impl.shim.mapreduce;

import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceFactory;
import org.pentaho.hadoop.shim.api.mapreduce.MapReduceService;
import org.pentaho.hadoop.shim.spi.HadoopShim;

import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Created by bryan on 7/6/15.
 */
public class MapReduceServiceFactoryImpl implements NamedClusterServiceFactory<MapReduceService> {
  private final HadoopShim hadoopShim;
  private final ExecutorService executorService;
  private final List<TransformationVisitorService> visitorServices;

  public MapReduceServiceFactoryImpl( HadoopShim hadoopShim, ExecutorService executorService,
                                      List<TransformationVisitorService> visitorServices ) {
    this.hadoopShim = hadoopShim;
    this.executorService = executorService;
    this.visitorServices = visitorServices;
  }

  @Override
  public Class<MapReduceService> getServiceClass() {
    return MapReduceService.class;
  }

  @Override
  public boolean canHandle( NamedCluster namedCluster ) {
    return namedCluster == null ? true : !namedCluster.isUseGateway();
  }

  @Override
  public MapReduceService create( NamedCluster namedCluster ) {
    return new MapReduceServiceImpl( namedCluster, hadoopShim, executorService, visitorServices );
  }
}

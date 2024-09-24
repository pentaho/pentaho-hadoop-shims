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

/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 * <p>
 * ******************************************************************************
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 ******************************************************************************/

package org.pentaho.big.data.impl.shim.pig;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceFactory;
import org.pentaho.bigdata.api.pig.PigService;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.pentaho.hadoop.shim.spi.PigShim;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by bryan on 7/6/15.
 */
public class PigServiceFactoryImpl implements NamedClusterServiceFactory<PigService> {
  private static final Logger LOGGER = LoggerFactory.getLogger( PigServiceFactoryImpl.class );
  private final HadoopShim hadoopShim;
  private final PigShim pigShim;

  public PigServiceFactoryImpl( HadoopShim hadoopShim, PigShim pigShim ) {
    this.hadoopShim = hadoopShim;
    this.pigShim = pigShim;
  }

  @Override public Class<PigService> getServiceClass() {
    return PigService.class;
  }

  @Override public boolean canHandle( NamedCluster namedCluster ) {
    String shimIdentifier = null; // TODO: Specify shim
    return true;
  }

  @Override public PigService create( NamedCluster namedCluster ) {
    return new PigServiceImpl( namedCluster, pigShim, hadoopShim );
  }
}

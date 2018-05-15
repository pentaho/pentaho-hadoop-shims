/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.impl.shim.oozie;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceFactory;
import org.pentaho.bigdata.api.oozie.OozieService;
import org.pentaho.oozie.shim.api.OozieClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OozieServiceFactoryImpl implements NamedClusterServiceFactory<OozieService> {
  private static final Logger LOGGER = LoggerFactory.getLogger( OozieServiceFactoryImpl.class );

  @Override public Class<OozieService> getServiceClass() {
    return OozieService.class;
  }

  @Override public boolean canHandle( NamedCluster namedCluster ) {
//    boolean ncState = namedCluster == null || !namedCluster.isUseGateway();
    return true;
  }

  @Override public OozieService create( NamedCluster namedCluster ) {
    String oozieUrl = namedCluster.getOozieUrl();
    OozieClient client = new FallbackOozieClientImpl(
      new org.apache.oozie.client.OozieClient( oozieUrl ) );
    return new OozieServiceImpl( client, namedCluster );
  }

}

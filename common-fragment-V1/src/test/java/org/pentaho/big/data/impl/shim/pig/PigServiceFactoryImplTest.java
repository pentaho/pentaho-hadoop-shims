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

package org.pentaho.big.data.impl.shim.pig;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.ConfigurationException;
import org.pentaho.hadoop.shim.api.pig.PigService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Created by bryan on 10/1/15.
 */
public class PigServiceFactoryImplTest {

  private PigServiceFactoryImpl pigServiceFactory;
  private boolean activeConfiguration;
  private NamedCluster namedCluster;

  private void initialize() throws ConfigurationException {
  }

  @Before
  public void setup() throws ConfigurationException {
    activeConfiguration = true;
    namedCluster = mock( NamedCluster.class );
    initialize();
  }

  @Test
  public void testGetService() {
    assertEquals( PigService.class, pigServiceFactory.getServiceClass() );
  }

  @Test
  public void testActiveCanHandle() {
    assertTrue( pigServiceFactory.canHandle( namedCluster ) );
  }

  @Test
  public void testInactiveCanHandle() throws ConfigurationException {
    activeConfiguration = false;
    initialize();
    //todo: fix knox handling
    //assertFalse( pigServiceFactory.canHandle( namedCluster ) );
  }

  @Test
  public void testCreateNoError() {
    assertTrue( pigServiceFactory.create( namedCluster ) instanceof PigServiceImpl );
  }

}

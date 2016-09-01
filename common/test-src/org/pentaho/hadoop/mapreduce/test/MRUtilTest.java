/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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

package org.pentaho.hadoop.mapreduce.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransConfiguration;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.TransMeta.TransformationType;
import org.pentaho.hadoop.mapreduce.MRUtil;

/**
 * Tests for {@link MRUtil}.
 */
public class MRUtilTest {
  private static final String USER_DIR = System.getProperty( "user.dir" );
  private static final String EXPECTED_DEFAULT_PLUGIN_DIR = USER_DIR + Const.FILE_SEPARATOR + "plugins";
  private static final TransMeta TRANS_META = getTestTransMeta();
  private static final TransConfiguration TRANS_EXEC_CONFIG = getTestTransExecConfig( TRANS_META );
  private Configuration c;
  private Trans trans;

  @Before
  public void setUp() {
    c = new Configuration();
  }

  @Test
  public void getPluginDirProperty() throws KettleException {
    assertNull( c.get( MRUtil.PROPERTY_PENTAHO_KETTLE_PLUGINS_DIR ) );
    String pluginDirProperty = MRUtil.getPluginDirProperty( c );
    assertTrue( "Plugin Directory Property not configured as expected: " + pluginDirProperty, pluginDirProperty
        .endsWith( EXPECTED_DEFAULT_PLUGIN_DIR ) );
  }

  @Test
  public void getPluginDirProperty_explicitly_set() throws KettleException {
    final String PLUGIN_DIR = "/opt/pentaho";
    // Working directory will be used for the plugin directory if it is not explicitly provided
    c.set( MRUtil.PROPERTY_PENTAHO_KETTLE_PLUGINS_DIR, PLUGIN_DIR );
    String pluginDirProperty = MRUtil.getPluginDirProperty( c );
    assertTrue( "Plugin Directory Property not configured as expected: " + pluginDirProperty, pluginDirProperty
        .endsWith( PLUGIN_DIR ) );
  }

  @Test
  public void getKettleHomeProperty() {
    String kettleHome = MRUtil.getKettleHomeProperty( c );
    assertEquals( USER_DIR, kettleHome );
  }

  @Test
  public void getKettleHomeProperty_explicitly_set() {
    final String KETTLE_HOME = "/my/kettle";
    // Working directory will be used for Kettle Home if it is not explicitly provided
    c.set( MRUtil.PROPERTY_PENTAHO_KETTLE_HOME, KETTLE_HOME );
    String kettleHome = MRUtil.getKettleHomeProperty( c );
    assertEquals( KETTLE_HOME, kettleHome );
  }

  @Test
  public void createTrans_normalEngine() throws Exception {
    // Reset transformationType from default value=TransformationType.Normal to the
    // TransformationType.SerialSingleThreaded to get more reliable use case
    TRANS_META.setTransformationType( TransformationType.SerialSingleThreaded );
    assertEquals( TransformationType.SerialSingleThreaded, TRANS_META.getTransformationType() );

    // get transformation with singleThreaded=false
    trans = MRUtil.getTrans( c, TRANS_EXEC_CONFIG.getXML(), false );
    assertNotNull( trans );
    assertEquals( TRANS_META.getName(), trans.getTransMeta().getName() );
    assertEquals( TransMeta.TransformationType.Normal, trans.getTransMeta().getTransformationType() );
  }

  @Test
  public void createTrans_singleThreaded() throws Exception {
    // Reset transformationType from default value=TransformationType.Normal to the
    // TransformationType.SerialSingleThreaded to get more reliable use case
    TRANS_META.setTransformationType( TransformationType.SerialSingleThreaded );
    assertEquals( TransformationType.SerialSingleThreaded, TRANS_META.getTransformationType() );

    // get transformation with singleThreaded=true
    trans = MRUtil.getTrans( c, TRANS_EXEC_CONFIG.getXML(), true );
    assertNotNull( trans );
    assertEquals( TRANS_META.getName(), trans.getTransMeta().getName() );
    assertEquals( TransMeta.TransformationType.SingleThreaded, trans.getTransMeta().getTransformationType() );
  }

  private static TransMeta getTestTransMeta() {
    TransMeta transMeta = new TransMeta();
    transMeta.setName( "Test transformation" );
    return transMeta;
  }

  private static TransConfiguration getTestTransExecConfig( TransMeta trMeta ) {
    TransExecutionConfiguration transExecConfig = new TransExecutionConfiguration();
    return new TransConfiguration( trMeta, transExecConfig );
  }
}

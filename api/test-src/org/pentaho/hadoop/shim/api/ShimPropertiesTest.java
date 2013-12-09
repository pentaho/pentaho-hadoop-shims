/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
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

package org.pentaho.hadoop.shim.api;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.hadoop.shim.api.ShimProperties.ListOverrideType;
import org.pentaho.hadoop.shim.api.ShimProperties.SetOverrideType;

public class ShimPropertiesTest {
  private ShimProperties shimProperties;

  @Before
  public void setup() {
    shimProperties = new ShimProperties();
  }

  private String join( String delim, Collection<String> collection ) {
    StringBuilder stringBuilder = new StringBuilder();
    for ( String string : collection ) {
      stringBuilder.append( string );
      stringBuilder.append( "," );
    }
    if ( stringBuilder.length() > 0 ) {
      stringBuilder.setLength( stringBuilder.length() - 1 );
    }
    return stringBuilder.toString();
  }

  @Test
  public void testGetConfigSetReplaceWithNoShimConfig() {
    Set<String> rootConfig = new HashSet<String>( Arrays.asList( "one", "b", "iii" ) );
    shimProperties.setProperty( "propName", join( ",", rootConfig ) );
    assertEquals( rootConfig, shimProperties.getConfigSet( "propName", SetOverrideType.REPLACE ) );
    shimProperties.setProperty( ShimProperties.SHIM_CP_CONFIG, "mr1" );
    assertEquals( rootConfig, shimProperties.getConfigSet( "propName", SetOverrideType.REPLACE ) );
  }

  @Test
  public void testGetConfigSetReplaceWithShimConfig() {
    Set<String> rootConfig = new HashSet<String>( Arrays.asList( "one", "b", "iii" ) );
    Set<String> shimConfig = new HashSet<String>( Arrays.asList( "1", "two", "tres" ) );
    shimProperties.setProperty( "propName", join( ",", rootConfig ) );
    shimProperties.setProperty( "mr1.propName", join( ",", shimConfig ) );
    shimProperties.setProperty( ShimProperties.SHIM_CP_CONFIG, "mr1" );
    assertEquals( shimConfig, shimProperties.getConfigSet( "propName", SetOverrideType.REPLACE ) );
  }

  @Test
  public void testGetConfigSetReplaceWithNoRootConfig() {
    Set<String> shimConfig = new HashSet<String>( Arrays.asList( "1", "two", "tres" ) );
    shimProperties.setProperty( "mr1.propName", join( ",", shimConfig ) );
    assertEquals( new HashSet<String>(), shimProperties.getConfigSet( "propName", SetOverrideType.REPLACE ) );
    shimProperties.setProperty( ShimProperties.SHIM_CP_CONFIG, "mr1" );
    assertEquals( shimConfig, shimProperties.getConfigSet( "propName", SetOverrideType.REPLACE ) );
  }

  @Test
  public void testGetConfigSetOverlayWithNoShimConfig() {
    Set<String> rootConfig = new HashSet<String>( Arrays.asList( "one", "b", "iii" ) );
    shimProperties.setProperty( "propName", join( ",", rootConfig ) );
    assertEquals( rootConfig, shimProperties.getConfigSet( "propName", SetOverrideType.OVERLAY ) );
    shimProperties.setProperty( ShimProperties.SHIM_CP_CONFIG, "mr1" );
    assertEquals( rootConfig, shimProperties.getConfigSet( "propName", SetOverrideType.OVERLAY ) );
  }

  @Test
  public void testGetConfigSetOverlayWithShimConfig() {
    Set<String> rootConfig = new HashSet<String>( Arrays.asList( "one", "b", "iii" ) );
    Set<String> shimConfig = new HashSet<String>( Arrays.asList( "1", "two", "tres" ) );
    Set<String> combinedConfig = new HashSet<String>( rootConfig );
    combinedConfig.addAll( shimConfig );
    shimProperties.setProperty( "propName", join( ",", rootConfig ) );
    shimProperties.setProperty( "mr1.propName", join( ",", shimConfig ) );
    shimProperties.setProperty( ShimProperties.SHIM_CP_CONFIG, "mr1" );
    assertEquals( combinedConfig, shimProperties.getConfigSet( "propName", SetOverrideType.OVERLAY ) );
  }

  @Test
  public void testGetConfigSetOverlayWithNoRootConfig() {
    Set<String> shimConfig = new HashSet<String>( Arrays.asList( "1", "two", "tres" ) );
    shimProperties.setProperty( "mr1.propName", join( ",", shimConfig ) );
    assertEquals( new HashSet<String>(), shimProperties.getConfigSet( "propName", SetOverrideType.OVERLAY ) );
    shimProperties.setProperty( ShimProperties.SHIM_CP_CONFIG, "mr1" );
    assertEquals( shimConfig, shimProperties.getConfigSet( "propName", SetOverrideType.OVERLAY ) );
  }

  @Test
  public void testGetConfigSetDefaultsToOverlay() {
    Set<String> rootConfig = new HashSet<String>( Arrays.asList( "one", "b", "iii" ) );
    Set<String> shimConfig = new HashSet<String>( Arrays.asList( "1", "two", "tres" ) );
    Set<String> combinedConfig = new HashSet<String>( rootConfig );
    combinedConfig.addAll( shimConfig );
    shimProperties.setProperty( "propName", join( ",", rootConfig ) );
    shimProperties.setProperty( "mr1.propName", join( ",", shimConfig ) );
    shimProperties.setProperty( ShimProperties.SHIM_CP_CONFIG, "mr1" );
    assertEquals( combinedConfig, shimProperties.getConfigSet( "propName" ) );
  }

  @Test
  public void testGetConfigListReplaceWithNoShimConfig() {
    List<String> rootConfig = new ArrayList<String>( Arrays.asList( "one", "b", "iii" ) );
    shimProperties.setProperty( "propName", join( ",", rootConfig ) );
    assertEquals( rootConfig, shimProperties.getConfigList( "propName", ListOverrideType.REPLACE ) );
    shimProperties.setProperty( ShimProperties.SHIM_CP_CONFIG, "mr1" );
    assertEquals( rootConfig, shimProperties.getConfigList( "propName", ListOverrideType.REPLACE ) );
  }

  @Test
  public void testGetConfigListReplaceWithShimConfig() {
    List<String> rootConfig = new ArrayList<String>( Arrays.asList( "one", "b", "iii" ) );
    List<String> shimConfig = new ArrayList<String>( Arrays.asList( "1", "two", "tres" ) );
    shimProperties.setProperty( "propName", join( ",", rootConfig ) );
    shimProperties.setProperty( "mr1.propName", join( ",", shimConfig ) );
    shimProperties.setProperty( ShimProperties.SHIM_CP_CONFIG, "mr1" );
    assertEquals( shimConfig, shimProperties.getConfigList( "propName", ListOverrideType.REPLACE ) );
  }

  @Test
  public void testGetConfigListReplaceWithNoRootConfig() {
    List<String> shimConfig = new ArrayList<String>( Arrays.asList( "1", "two", "tres" ) );
    shimProperties.setProperty( "mr1.propName", join( ",", shimConfig ) );
    assertEquals( new ArrayList<String>(), shimProperties.getConfigList( "propName", ListOverrideType.REPLACE ) );
    shimProperties.setProperty( ShimProperties.SHIM_CP_CONFIG, "mr1" );
    assertEquals( shimConfig, shimProperties.getConfigList( "propName", ListOverrideType.REPLACE ) );
  }

  @Test
  public void testGetConfigListAppendWithNoShimConfig() {
    List<String> rootConfig = new ArrayList<String>( Arrays.asList( "one", "b", "iii" ) );
    shimProperties.setProperty( "propName", join( ",", rootConfig ) );
    assertEquals( rootConfig, shimProperties.getConfigList( "propName", ListOverrideType.APPEND ) );
    shimProperties.setProperty( ShimProperties.SHIM_CP_CONFIG, "mr1" );
    assertEquals( rootConfig, shimProperties.getConfigList( "propName", ListOverrideType.APPEND ) );
  }

  @Test
  public void testGetConfigListAppendWithShimConfig() {
    List<String> rootConfig = new ArrayList<String>( Arrays.asList( "one", "b", "iii" ) );
    List<String> shimConfig = new ArrayList<String>( Arrays.asList( "1", "two", "tres" ) );
    List<String> combinedList = new ArrayList<String>( rootConfig );
    combinedList.addAll( shimConfig );
    shimProperties.setProperty( "propName", join( ",", rootConfig ) );
    shimProperties.setProperty( "mr1.propName", join( ",", shimConfig ) );
    shimProperties.setProperty( ShimProperties.SHIM_CP_CONFIG, "mr1" );
    assertEquals( combinedList, shimProperties.getConfigList( "propName", ListOverrideType.APPEND ) );
  }

  @Test
  public void testGetConfigListAppendWithNoRootConfig() {
    List<String> shimConfig = new ArrayList<String>( Arrays.asList( "1", "two", "tres" ) );
    shimProperties.setProperty( "mr1.propName", join( ",", shimConfig ) );
    assertEquals( new ArrayList<String>(), shimProperties.getConfigList( "propName", ListOverrideType.APPEND ) );
    shimProperties.setProperty( ShimProperties.SHIM_CP_CONFIG, "mr1" );
    assertEquals( shimConfig, shimProperties.getConfigList( "propName", ListOverrideType.APPEND ) );
  }

  @Test
  public void testGetConfigListPrependWithNoShimConfig() {
    List<String> rootConfig = new ArrayList<String>( Arrays.asList( "one", "b", "iii" ) );
    shimProperties.setProperty( "propName", join( ",", rootConfig ) );
    assertEquals( rootConfig, shimProperties.getConfigList( "propName", ListOverrideType.PREPEND ) );
    shimProperties.setProperty( ShimProperties.SHIM_CP_CONFIG, "mr1" );
    assertEquals( rootConfig, shimProperties.getConfigList( "propName", ListOverrideType.PREPEND ) );
  }

  @Test
  public void testGetConfigListPrependWithShimConfig() {
    List<String> rootConfig = new ArrayList<String>( Arrays.asList( "one", "b", "iii" ) );
    List<String> shimConfig = new ArrayList<String>( Arrays.asList( "1", "two", "tres" ) );
    List<String> combinedList = new ArrayList<String>( shimConfig );
    combinedList.addAll( rootConfig );
    shimProperties.setProperty( "propName", join( ",", rootConfig ) );
    shimProperties.setProperty( "mr1.propName", join( ",", shimConfig ) );
    shimProperties.setProperty( ShimProperties.SHIM_CP_CONFIG, "mr1" );
    assertEquals( combinedList, shimProperties.getConfigList( "propName", ListOverrideType.PREPEND ) );
  }

  @Test
  public void testGetConfigListPrependWithNoRootConfig() {
    List<String> shimConfig = new ArrayList<String>( Arrays.asList( "1", "two", "tres" ) );
    shimProperties.setProperty( "mr1.propName", join( ",", shimConfig ) );
    assertEquals( new ArrayList<String>(), shimProperties.getConfigList( "propName", ListOverrideType.PREPEND ) );
    shimProperties.setProperty( ShimProperties.SHIM_CP_CONFIG, "mr1" );
    assertEquals( shimConfig, shimProperties.getConfigList( "propName", ListOverrideType.PREPEND ) );
  }

  @Test
  public void testGetConfigListDefaultsToAppend() {
    List<String> rootConfig = new ArrayList<String>( Arrays.asList( "one", "b", "iii" ) );
    List<String> shimConfig = new ArrayList<String>( Arrays.asList( "1", "two", "tres" ) );
    List<String> combinedList = new ArrayList<String>( rootConfig );
    combinedList.addAll( shimConfig );
    shimProperties.setProperty( "propName", join( ",", rootConfig ) );
    shimProperties.setProperty( "mr1.propName", join( ",", shimConfig ) );
    shimProperties.setProperty( ShimProperties.SHIM_CP_CONFIG, "mr1" );
    assertEquals( combinedList, shimProperties.getConfigList( "propName" ) );
  }
}

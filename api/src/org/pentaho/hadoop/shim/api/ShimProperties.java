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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Subclass of java.util.Properties with support for overriding properties with different config values (e.g. mr1 vs mr2
 * config differences)
 */
public class ShimProperties extends Properties {
  private static final long serialVersionUID = 2033564331119378266L;

  public static final String SHIM_CP_CONFIG = "shim.current.config";

  public static enum ListOverrideType {
    REPLACE, APPEND, PREPEND
  }

  public static enum SetOverrideType {
    REPLACE, OVERLAY
  }

  private List<String> getShimConfigs() {
    List<String> shimConfigs = new ArrayList<String>();
    String shimCurrentConfig = getProperty( SHIM_CP_CONFIG );
    if ( shimCurrentConfig != null ) {
      for ( String config : shimCurrentConfig.trim().split( "," ) ) {
        shimConfigs.add( config.trim() );
      }
    }
    return shimConfigs;
  }

  private List<String> getShimConfigProperties( String property ) {
    List<String> shimConfigProperties = new ArrayList<String>();
    for ( String config : getShimConfigs() ) {
      shimConfigProperties.add( getProperty( config + "." + property ) );
    }
    return shimConfigProperties;
  }

  /**
   * Gets a list from a comma separated property with support for overrides, defaulting to Append behavior
   * 
   * @param property
   *          the property
   * @return the list
   */
  public List<String> getConfigList( String property ) {
    return getConfigList( property, ListOverrideType.APPEND );
  }

  /**
   * Gets a list from a comma separated property with support for overrides
   * 
   * @param property
   *          the property
   * @param listOverrideType
   *          the override type
   * @return the list
   */
  public List<String> getConfigList( String property, ListOverrideType listOverrideType ) {
    List<String> shimConfigValues = getShimConfigProperties( property );
    List<String> shimProperties = new ArrayList<String>();

    String globalValues = getProperty( property );
    if ( globalValues != null && globalValues.trim().length() > 0 ) {
      shimProperties.addAll( Arrays.asList( globalValues.split( "," ) ) );
    }

    for ( String shimConfigValue : shimConfigValues ) {
      if ( shimConfigValue != null ) {
        List<String> shimProperty = new ArrayList<String>();
        if ( shimConfigValue.trim().length() > 0 ) {
          for ( String prop : shimConfigValue.trim().split( "," ) ) {
            shimProperty.add( prop.trim() );
          }
        }
        switch ( listOverrideType ) {
          case APPEND:
            shimProperties.addAll( shimProperty );
            break;
          case PREPEND:
            shimProperty.addAll( shimProperties );
            shimProperties = shimProperty;
            break;
          case REPLACE:
            shimProperties = shimProperty;
            break;
        }
      }
    }

    return shimProperties;
  }

  /**
   * Gets a list from a comma separated property with support for overrides, defaulting to Overlay behavior
   * 
   * @param property
   *          the property
   * @return the list
   */
  public Set<String> getConfigSet( String property ) {
    return getConfigSet( property, SetOverrideType.OVERLAY );
  }

  /**
   * Gets a list from a comma separated property with support for overrides
   * 
   * @param property
   *          the property
   * @param overrideType
   *          the override type
   * @return the list
   */
  public Set<String> getConfigSet( String property, SetOverrideType overrideType ) {
    List<String> shimConfigValues = getShimConfigProperties( property );
    Set<String> shimProperties = new HashSet<String>();

    String globalValues = getProperty( property );
    if ( globalValues != null && globalValues.trim().length() > 0 ) {
      shimProperties.addAll( Arrays.asList( globalValues.split( "," ) ) );
    }

    for ( String shimConfigValue : shimConfigValues ) {
      if ( shimConfigValue != null ) {
        Set<String> shimProperty = new HashSet<String>();
        if ( shimConfigValue.trim().length() > 0 ) {
          for ( String prop : shimConfigValue.trim().split( "," ) ) {
            shimProperty.add( prop.trim() );
          }
        }
        switch ( overrideType ) {
          case OVERLAY:
            shimProperties.addAll( shimProperty );
            break;
          case REPLACE:
            shimProperties = shimProperty;
            break;
        }
      }
    }

    return shimProperties;
  }

  /**
   * Returns a map of key -> value of all shim properties with the given prefix (the prefix is removed)
   * 
   * @param prefix
   *          the prefix to look for
   * @return a map of key -> value of all shim properties with the given prefix (the prefix is removed)
   */
  public Map<String, String> getPrefixedProperties( String prefix ) {
    List<String> propertyPrefixes = new ArrayList<String>();
    propertyPrefixes.add( prefix + "." );
    for ( String shimConfig : getShimConfigs() ) {
      propertyPrefixes.add( shimConfig + "." + prefix.trim() + "." );
    }
    Map<String, String> prefixedProperties = new HashMap<String, String>();
    for ( String currentPrefix : propertyPrefixes ) {
      for ( String propertyName : stringPropertyNames() ) {
        if ( propertyName.startsWith( currentPrefix ) ) {
          prefixedProperties.put( propertyName.substring( currentPrefix.length() ), getProperty( propertyName ) );
        }
      }
    }
    return prefixedProperties;
  }
}

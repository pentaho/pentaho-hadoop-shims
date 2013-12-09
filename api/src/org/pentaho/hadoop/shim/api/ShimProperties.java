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
import java.util.HashSet;
import java.util.List;
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

  private String getShimConfigProperty( String property ) {
    String shimConfigProperty = null;
    String shimClasspathConfig = getProperty( SHIM_CP_CONFIG );
    if ( shimClasspathConfig != null ) {
      shimConfigProperty = getProperty( shimClasspathConfig + "." + property );
    }
    return shimConfigProperty;
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
    List<String> rootProperty = new ArrayList<String>();
    String shimConfigValues = getShimConfigProperty( property );

    if ( listOverrideType != ListOverrideType.REPLACE || shimConfigValues == null ) {
      String globalValues = getProperty( property );
      if ( globalValues != null && globalValues.trim().length() > 0 ) {
        rootProperty = new ArrayList<String>( Arrays.asList( globalValues.split( "," ) ) );
      }
    }

    List<String> shimProperty = new ArrayList<String>();
    if ( shimConfigValues != null && shimConfigValues.trim().length() > 0 ) {
      shimProperty = new ArrayList<String>( Arrays.asList( shimConfigValues.split( "," ) ) );
    }

    if ( rootProperty != null && ( listOverrideType != ListOverrideType.REPLACE || shimConfigValues == null ) ) {
      switch ( listOverrideType ) {
        case APPEND:
          rootProperty.addAll( shimProperty );
          return rootProperty;
        case PREPEND:
          shimProperty.addAll( rootProperty );
          return shimProperty;
        case REPLACE:
          return rootProperty;
      }
    }

    return shimProperty;
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
    Set<String> result = new HashSet<String>();
    String shimConfigValues = getShimConfigProperty( property );

    if ( overrideType == SetOverrideType.OVERLAY || shimConfigValues == null ) {
      String globalValues = getProperty( property );
      if ( globalValues != null && globalValues.trim().length() > 0 ) {
        for ( String folder : globalValues.split( "," ) ) {
          result.add( folder );
        }
      }
    }

    if ( shimConfigValues != null && shimConfigValues.trim().length() > 0 ) {
      for ( String folder : shimConfigValues.split( "," ) ) {
        result.add( folder );
      }
    }

    return result;
  }
}

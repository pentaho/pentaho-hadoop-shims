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

package org.pentaho.big.data.api.jdbc.impl;

import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.jdbc.JdbcUrl;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.osgi.metastore.locator.api.MetastoreLocator;

import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Created by bryan on 4/4/16.
 */
public class JdbcUrlImpl implements JdbcUrl {
  public static final String PENTAHO_NAMED_CLUSTER = "pentahoNamedCluster";
  private String scheme;
  private String host;
  private String port;
  private String path;
  private final Map<String, String> queryParams;
  private final NamedClusterService namedClusterService;
  private final MetastoreLocator metastoreLocator;
  private Pattern uriPattern = Pattern.compile( "^(.*)://([^:]*):?(\\d*)?/(.*)$" );

  public JdbcUrlImpl( String url, NamedClusterService namedClusterService, MetastoreLocator metastoreLocator )
    throws URISyntaxException {
    this.namedClusterService = namedClusterService;
    this.metastoreLocator = metastoreLocator;
    if ( !url.startsWith( "jdbc:" ) ) {
      throw new URISyntaxException( url, "Should start with \"jdbc:\"" );
    }
    Matcher m = uriPattern.matcher( url.substring( 5 ) );
    if ( m.matches() ) {
      scheme = m.group( 1 );
      host = m.group( 2 );
      port = m.group( 3 );
      path = m.group( 4 );
      String query = null;
      if ( path != null ) {
        int beginIndex = path.indexOf( ';' );
        if ( beginIndex >= 0 ) {
          query = path.substring( beginIndex );
        }
      }
      if ( query == null ) {
        queryParams = new HashMap<>();
      } else {
        queryParams = queryStringToParamMap( query );
      }
    } else {
      throw new URISyntaxException( url, "Could not parse URL" );
    }
  }

  private Map<String, String> queryStringToParamMap( String query ) {
    return Arrays.asList( query.split( ";" ) ).stream()
      .map( s -> {
        int i = s.indexOf( '=' );
        if ( i < 0 || i >= s.length() - 1 ) {
          return null;
        }
        return new String[] { s.substring( 0, i ), s.substring( i + 1 ) };
      } )
      .filter( Objects::nonNull )
      .collect( Collectors.toMap( r -> r[ 0 ], t -> t[ 1 ] ) );
  }

  @Override public String toString() {
    String queryParameters = queryParams.entrySet().stream()
      .map( entry -> entry.getKey() + "=" + entry.getValue() )
      .filter( Objects::nonNull )
      .sorted( String::compareToIgnoreCase )
      .collect( Collectors.joining( ";" ) );
    String tempPath = path;
    int semicolon = tempPath.indexOf( ';' );
    if ( semicolon >= 0 ) {
      tempPath = tempPath.substring( 0, semicolon );
    }
    return "jdbc:" + scheme + "://" + host
      + ( port == null || port.equals( "" ) ? "" : ":" + port ) + "/"
      + tempPath + ( queryParameters != null && queryParameters.length() > 0 ? ";" + queryParameters : "" );
  }

  @Override public void setQueryParam( String key, String value ) {
    queryParams.put( key, value );
  }

  @Override public String getQueryParam( String key ) {
    return queryParams.get( key );
  }

  @Override public NamedCluster getNamedCluster()
    throws MetaStoreException {
    IMetaStore metaStore = metastoreLocator.getMetastore();
    if ( metaStore == null ) {
      return null;
    }
    String queryParam = getQueryParam( PENTAHO_NAMED_CLUSTER );
    if ( queryParam == null ) {
      return null;
    }
    return namedClusterService.read( queryParam, metaStore );
  }

  @Override
  public String getHost() {
    return host;
  }
}

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

package org.pentaho.hadoop.shim.common;

import java.net.URI;
import java.net.URISyntaxException;
import java.text.MessageFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HiveSQLUtils {

  private static final Pattern INSERT_SQL_PATTERN = Pattern.compile(
    "INSERT\\s+INTO\\s+(?!TABLE\\s+)\\s*(\\w+(.\\w+)?)\\s*(\\([^\\)]+\\))?\\s*VALUES\\s+(.*)",
    Pattern.CASE_INSENSITIVE );

  private static final String HIVE_INSERT_SQL_FORMAT = "INSERT INTO TABLE {0} VALUES {1}";

  public static String processSQLString( String sql ) {
    Matcher matcher = INSERT_SQL_PATTERN.matcher( sql.trim() );
    if ( matcher.matches() ) {
      String tableName = matcher.group( 1 );
      String values = matcher.group( 4 );
      return MessageFormat.format( HIVE_INSERT_SQL_FORMAT, tableName, values );
    }
    return sql;
  }

  public static String getDatabaseNameFromURL( String connectUrl ) throws URISyntaxException {
    if ( !connectUrl.startsWith( "jdbc:hive" ) ) {
      return "";
    }

    URI uri = new URI( connectUrl.substring( connectUrl.indexOf( "hive" ) ) );
    String path = uri.getPath();
    String dbName = "";
    if ( path != null && !path.isEmpty() ) {
      if ( path.contains( ";" ) ) {
        dbName = path.substring( 1, path.indexOf( ';' ) );
      } else {
        dbName = path.substring( 1 );
      }
    }

    return dbName;
  }
}

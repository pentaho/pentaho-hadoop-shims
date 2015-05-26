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

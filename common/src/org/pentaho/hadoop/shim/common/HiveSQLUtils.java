package org.pentaho.hadoop.shim.common;

import java.text.MessageFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HiveSQLUtils {

  private static final Pattern INSERT_SQL_PATTERN = Pattern.compile(
          "INSERT\\s+INTO\\s+(?!TABLE\\s+)\\s*(\\w+(.\\w+)?)\\s*(\\([^\\)]+\\))?\\s*VALUES\\s+(.*)",
          Pattern.CASE_INSENSITIVE);

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
}

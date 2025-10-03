/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.hadoop.shim.common;

import org.junit.Test;

import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;
import static org.pentaho.hadoop.shim.common.HiveSQLUtils.getDatabaseNameFromURL;

public class HiveSQLUtilsTest {
  @Test
  public void shouldAddTableKeywordInInsertStatement() throws Exception {
    String sql = "INSERT INTO tablename VALUES (value1, value2)";
    String processedSql = HiveSQLUtils.processSQLString( sql );
    assertEquals( "INSERT INTO TABLE tablename VALUES (value1, value2)", processedSql );
  }

  @Test
  public void shouldAcceptSeveralSpaceChars() throws Exception {
    String sql = " INSERT   INTO    tablename   VALUES    (   value1,   value2)";
    String processedSql = HiveSQLUtils.processSQLString( sql );
    // "as-is" spaces in values
    assertEquals( "INSERT INTO TABLE tablename VALUES (   value1,   value2)", processedSql );
  }

  @Test
  public void shouldIgnoreColumnNamesInInsertStatement() throws Exception {
    String sql = "INSERT INTO tablename (column1, column2) VALUES (value1, value2)";
    String processedSql = HiveSQLUtils.processSQLString( sql );
    assertEquals( "INSERT INTO TABLE tablename VALUES (value1, value2)", processedSql );
  }

  @Test
  public void shouldUseAllProvidedValues() throws Exception {
    String sql = "INSERT INTO tablename (column1, column2) VALUES (value11, value12), (value21, value22)";
    String processedSql = HiveSQLUtils.processSQLString( sql );
    assertEquals( "INSERT INTO TABLE tablename VALUES (value11, value12), (value21, value22)", processedSql );
  }

  @Test
  public void shouldUseSchemaAndTableName() throws Exception {
    String sql = "INSERT INTO schema.tablename (column1, column2) values (value1, value2)";
    String processedSql = HiveSQLUtils.processSQLString( sql );
    assertEquals( "INSERT INTO TABLE schema.tablename VALUES (value1, value2)", processedSql );
  }

  @Test
  public void shouldReturnOriginalStringIfNotMatches() throws Exception {
    String sql = "SELECT * FROM tablename";
    String processedSql = HiveSQLUtils.processSQLString( sql );
    assertEquals( sql, processedSql );
  }

  @Test
  public void shouldReturnOriginalStringIfInsertIntoTableUsed() throws Exception {
    String sql = "INSERT INTO TABLE - whatever - new hive syntax is used";
    String processedSql = HiveSQLUtils.processSQLString( sql );
    assertEquals( sql, processedSql );
  }

  @Test
  public void testGetDatabaseNameFromURL() throws URISyntaxException {
    assertEquals( "", getDatabaseNameFromURL( "jdbc:mysql://host:10000/dbName" ) );
    assertEquals( "", getDatabaseNameFromURL( "jdbc:hive://host:10000" ) );
    assertEquals( "dbName", getDatabaseNameFromURL( "jdbc:hive2://host:10000/dbName" ) );
    assertEquals( "dbName", getDatabaseNameFromURL( "jdbc:hive2://host:21051/dbName;principal=impala/host@realm" ) );
  }
}

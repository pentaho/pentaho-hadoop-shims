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

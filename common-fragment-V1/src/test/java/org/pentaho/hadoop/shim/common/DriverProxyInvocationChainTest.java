/*! ******************************************************************************
 *
 * Pentaho Data Integration
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

package org.pentaho.hadoop.shim.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.hive.jdbc.HiveDriver;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DriverProxyInvocationChainTest {

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    DriverProxyInvocationChain.driverProxyClassLoader = DriverProxyInvocationChainTest.class.getClassLoader();
  }

  @AfterClass
  public static void teardownAfterClass() {

  }

  @Before
  public void setup() throws Exception {
    DriverProxyInvocationChain.init();
  }

  @After
  public void teardown() {

  }

  @Test
  public void testInitCompletes() {
    DriverProxyInvocationChain.setInitialized( false );
    assertFalse( DriverProxyInvocationChain.isInitialized() );
    DriverProxyInvocationChain.init();
    assertTrue( DriverProxyInvocationChain.isInitialized() );
  }

  @Test
  public void testInitHive1Classes() {
    assertTrue( DriverProxyInvocationChain.isInitialized() );
    if ( Boolean.parseBoolean( System.getProperty( "org.pentaho.hadoop.shims.check.hive1", "true" ) ) ) {
      assertNotNull( DriverProxyInvocationChain.hive1DbMetaDataClass );
      assertNotNull( DriverProxyInvocationChain.hive1ResultSetClass );
      assertNotNull( DriverProxyInvocationChain.hive1ClientClass );
      assertNotNull( DriverProxyInvocationChain.hive1StatementClass );
    }
  }

  @Test
  public void testGetProxyNotNull() throws Exception {
    assertTrue( DriverProxyInvocationChain.isInitialized() );
    if ( Boolean.parseBoolean( System.getProperty( "org.pentaho.hadoop.shims.check.hive1", "true" ) ) ) {
      // Create Hive driver
      Driver hiveDriver = new HiveDriver();
      // Create proxy to driver
      Driver driver = DriverProxyInvocationChain.getProxy( Driver.class, hiveDriver );
      assertNotNull( driver );
    }
  }

  @Test
  public void testHiveConnectionIsReadOnly() throws SQLException {
    assertTrue( DriverProxyInvocationChain.isInitialized() );
    // Create Hive driver
    Driver hiveDriver = mock( HiveDriver.class );

    // Create proxy to driver
    Driver proxiedDriver = DriverProxyInvocationChain.getProxy( Driver.class, hiveDriver );

    String URL = "jdbc:hive://localhost:8020/default";

    // Get mock of original connection, inject the usual SQLException (Method not supported)
    Connection connectionMock = mock( Connection.class );
    doReturn( mock( Statement.class ) ).when( connectionMock ).createStatement();

    doReturn( connectionMock ).when( hiveDriver ).connect( URL, null );
    Connection hiveConnection = hiveDriver.connect( URL, null );
    assertNotNull( "The real Hive connection should be valid!", hiveConnection );
    doThrow( new SQLException( "Method not supported" ) ).when( hiveConnection ).isReadOnly();

    // Get connection via proxy
    Connection proxiedConnection = proxiedDriver.connect( URL, null );
    assertNotNull( "The proxied Hive connection should be valid!", proxiedConnection );

    // Allow a SQLException with "Method not supported" for the original driver
    try {
      hiveConnection.isReadOnly();
    } catch ( SQLException sqlException ) {
      assertEquals( sqlException.getMessage(), "Method not supported" );
    }

    // Do not allow a SQLException for the proxied driver
    try {
      assertFalse( proxiedConnection.isReadOnly() );
    } catch ( SQLException sqlException ) {
      fail( "No exception should be thrown for isReadOnly(), expecting false" );
    }
  }

  @Test
  public void testDatabaseSelected() throws SQLException {
    Driver driverMock = mock( HiveDriver.class );
    Driver driverProxy = DriverProxyInvocationChain.getProxy( Driver.class, driverMock );

    Connection connectionMock = mock( Connection.class );
    doReturn( connectionMock ).when( driverMock ).connect( anyString(), (Properties) isNull() );

    Statement statementMock = mock( Statement.class );
    doReturn( statementMock ).when( connectionMock ).createStatement();

    driverProxy.connect( "jdbc:hive://host:port/dbName", null );
    verify( statementMock ).execute( "use dbName" );
  }

  @Test
  public void testGetTablesWithSchema() throws SQLException {
    Class hive2;
    try {
      hive2 = Class.forName( "org.apache.hive.jdbc.HiveDatabaseMetaData" );
    } catch ( ClassNotFoundException e ) {
      return;
    }
    if ( hive2 != null ) {
      Driver driverMock = mock( HiveDriver.class );
      Driver driverProxy = DriverProxyInvocationChain.getProxy( Driver.class, driverMock );

      Connection connectionMock = mock( Connection.class );
      doReturn( connectionMock ).when( driverMock ).connect( anyString(), (Properties) isNull() );

      Statement statementMock = mock( Statement.class );
      doReturn( statementMock ).when( connectionMock ).createStatement();

      ResultSet resultSet = mock( ResultSet.class );
      doReturn( resultSet ).when( statementMock ).executeQuery( anyString() );

      DatabaseMetaData databaseMetaDataMock = (DatabaseMetaData) mock( hive2 );
      doReturn( databaseMetaDataMock ).when( connectionMock ).getMetaData();

      String schema = "someSchema";
      doThrow( new SQLException( "Method is not supported" ) ).when( databaseMetaDataMock )
        .getTables( null, schema, null, null );

      Connection conn = driverProxy.connect( "jdbc:hive://host:port/dbName", null );

      conn.getMetaData().getTables( null, schema, null, null );
      verify( statementMock ).execute( "use dbName" );
      verify( statementMock ).executeQuery( "show tables in " + schema );
    }
  }
}

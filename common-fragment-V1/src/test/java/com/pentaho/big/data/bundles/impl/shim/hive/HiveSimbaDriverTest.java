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


package com.pentaho.big.data.bundles.impl.shim.hive;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.hadoop.shim.api.jdbc.JdbcUrlParser;

import java.sql.Driver;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by bryan on 4/18/16.
 */
@RunWith( MockitoJUnitRunner.class )
public class HiveSimbaDriverTest {
  @Mock Driver delegate;
  @Mock JdbcUrlParser jdbcUrlParser;
  private HiveSimbaDriver hiveSimbaDriver;

  @Before
  public void setup() {
    hiveSimbaDriver = new HiveSimbaDriver( delegate, null, true, jdbcUrlParser );
  }

  @Test
  public void testCheckBeforeCallActiveDriverNoSimbaParam() throws SQLException {
    assertNull( hiveSimbaDriver.checkBeforeCallActiveDriver( "jdbc:hive2:a" ) );
  }

  @Test
  public void testCheckBeforeCallActiveDriverHiveMatchMissing() throws SQLException {
    assertNull(
      hiveSimbaDriver.checkBeforeCallActiveDriver( "jdbc:hive:a;" + HiveSimbaDriver.SIMBA_SPECIFIC_URL_PARAMETER ) );
  }

  @Test
  public void testCheckBeforeCallActiveDriver() throws SQLException {
    assertEquals( delegate,
      hiveSimbaDriver.checkBeforeCallActiveDriver( "jdbc:hive2:a;" + HiveSimbaDriver.SIMBA_SPECIFIC_URL_PARAMETER ) );
  }
}

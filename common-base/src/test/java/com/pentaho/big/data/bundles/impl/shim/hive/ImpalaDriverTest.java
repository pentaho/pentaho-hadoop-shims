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

import static org.junit.Assert.assertTrue;

/**
 * Created by bryan on 4/18/16.
 */
@RunWith( MockitoJUnitRunner.class )
public class ImpalaDriverTest {
  @Mock Driver delegate;
  @Mock JdbcUrlParser jdbcUrlParser;
  private ImpalaDriver impalaDriver;

  @Before
  public void setup() {
    impalaDriver = new ImpalaDriver( delegate, null, true, jdbcUrlParser );
  }

  @Test
  public void testConstructor() {
    assertTrue( impalaDriver instanceof ImpalaDriver );
  }
}

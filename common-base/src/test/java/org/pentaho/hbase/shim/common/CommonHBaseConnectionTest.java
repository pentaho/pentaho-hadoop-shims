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

package org.pentaho.hbase.shim.common;

import org.junit.Test;
import org.pentaho.hbase.shim.common.CommonHBaseConnection;

import static org.junit.Assert.assertEquals;

/**
 * Created by Vasilina_Terehova on 4/10/2018.
 */
public class CommonHBaseConnectionTest {

  @Test
  public void testMajorVersion() {
    assertEquals( 7, (int) CommonHBaseConnection.getMajorVersionNumber( "Mapr 78" ) );
    assertEquals( 6, (int) CommonHBaseConnection.getMajorVersionNumber( "Mapr 6.0" ) );
    assertEquals( 5, (int) CommonHBaseConnection.getMajorVersionNumber( "Mapr 5.2" ) );
    assertEquals( 5, (int) CommonHBaseConnection.getMajorVersionNumber( "Mapr 5" ) );
    assertEquals( 5, (int) CommonHBaseConnection.getMajorVersionNumber( "Mapr5" ) );
  }

  @Test
  public void testIsMaprAbove6() {
    assertEquals( false, CommonHBaseConnection.isMapR60OrAboveShim( "Cdh 6.0" ) );
    assertEquals( false, CommonHBaseConnection.isMapR60OrAboveShim( "Map 6.0" ) );
    assertEquals( true, CommonHBaseConnection.isMapR60OrAboveShim( "Mapr 6.0" ) );
    assertEquals( true, CommonHBaseConnection.isMapR60OrAboveShim( "MapR 6.0" ) );
    assertEquals( true, CommonHBaseConnection.isMapR60OrAboveShim( "MAPR 6.0" ) );
    assertEquals( false, CommonHBaseConnection.isMapR60OrAboveShim( "Mapr 5.2" ) );
    assertEquals( false, CommonHBaseConnection.isMapR60OrAboveShim( "Mapr 5" ) );
    assertEquals( false, CommonHBaseConnection.isMapR60OrAboveShim( "Mapr5" ) );
    assertEquals( true, CommonHBaseConnection.isMapR60OrAboveShim( "Mapr 7" ) );
  }
}

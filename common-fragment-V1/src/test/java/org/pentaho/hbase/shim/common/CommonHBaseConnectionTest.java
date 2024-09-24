/*******************************************************************************
 * Pentaho Big Data
 * <p/>
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
 * <p/>
 * ******************************************************************************
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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

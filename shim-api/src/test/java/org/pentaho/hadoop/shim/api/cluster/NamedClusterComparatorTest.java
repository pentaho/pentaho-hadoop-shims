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


package org.pentaho.hadoop.shim.api.cluster;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 8/28/15.
 */
public class NamedClusterComparatorTest {
  private NamedCluster namedCluster1;
  private NamedCluster namedCluster2;
  private String firstName;
  private String secondName;

  @Before
  public void setup() {
    firstName = "a";
    secondName = "b";

    namedCluster1 = Mockito.mock( NamedCluster.class );
    namedCluster2 = Mockito.mock( NamedCluster.class );
  }

  @Test
  public void testFirstNameFirst() {
    Mockito.when( namedCluster1.getName() ).thenReturn( firstName );
    Mockito.when( namedCluster2.getName() ).thenReturn( secondName );
    assertTrue( "Expected " + firstName + " before " + secondName,
      NamedCluster.comparator.compare( namedCluster1, namedCluster2 ) < 0 );
    assertTrue( "Expected " + secondName + " after " + firstName,
      NamedCluster.comparator.compare( namedCluster2, namedCluster1 ) > 0 );
  }

  @Test
  public void testFirstNameFirstFirstUpper() {
    String firstUpper = firstName.toUpperCase();
    Mockito.when( namedCluster1.getName() ).thenReturn( firstUpper );
    Mockito.when( namedCluster2.getName() ).thenReturn( secondName );
    assertTrue( "Expected " + firstUpper + " before " + secondName,
      NamedCluster.comparator.compare( namedCluster1, namedCluster2 ) < 0 );
    assertTrue( "Expected " + secondName + " after " + firstUpper,
      NamedCluster.comparator.compare( namedCluster2, namedCluster1 ) > 0 );
  }

  @Test
  public void testFirstNameFirstSecondUpper() {
    String secondUpper = secondName.toUpperCase();
    Mockito.when( namedCluster1.getName() ).thenReturn( firstName );
    Mockito.when( namedCluster2.getName() ).thenReturn( secondUpper );
    assertTrue( "Expected " + firstName + " before " + secondUpper,
      NamedCluster.comparator.compare( namedCluster1, namedCluster2 ) < 0 );
    assertTrue( "Expected " + secondUpper + " after " + firstName,
      NamedCluster.comparator.compare( namedCluster2, namedCluster1 ) > 0 );
  }

  @Test
  public void testFirstNameFirstBothUpper() {
    String firstUpper = firstName.toUpperCase();
    String secondUpper = secondName.toUpperCase();
    Mockito.when( namedCluster1.getName() ).thenReturn( firstUpper );
    Mockito.when( namedCluster2.getName() ).thenReturn( secondUpper );
    assertTrue( "Expected " + firstUpper + " before " + secondUpper,
      NamedCluster.comparator.compare( namedCluster1, namedCluster2 ) < 0 );
    assertTrue( "Expected " + secondUpper + " after " + firstUpper,
      NamedCluster.comparator.compare( namedCluster2, namedCluster1 ) > 0 );
  }

  @Test
  public void testEqual() {
    Mockito.when( namedCluster1.getName() ).thenReturn( firstName );
    Mockito.when( namedCluster2.getName() ).thenReturn( firstName );
    assertEquals( 0, NamedCluster.comparator.compare( namedCluster1, namedCluster2 ) );
    assertEquals( 0, NamedCluster.comparator.compare( namedCluster2, namedCluster1 ) );
  }

  @Test
  public void testEqualOneUpper() {
    Mockito.when( namedCluster1.getName() ).thenReturn( firstName.toUpperCase() );
    Mockito.when( namedCluster2.getName() ).thenReturn( firstName );
    assertEquals( 0, NamedCluster.comparator.compare( namedCluster1, namedCluster2 ) );
    assertEquals( 0, NamedCluster.comparator.compare( namedCluster2, namedCluster1 ) );
  }

  @Test
  public void testEqualBothUpper() {
    Mockito.when( namedCluster1.getName() ).thenReturn( firstName.toUpperCase() );
    Mockito.when( namedCluster2.getName() ).thenReturn( firstName.toUpperCase() );
    assertEquals( 0, NamedCluster.comparator.compare( namedCluster1, namedCluster2 ) );
    assertEquals( 0, NamedCluster.comparator.compare( namedCluster2, namedCluster1 ) );
  }
}

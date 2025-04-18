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


package org.pentaho.hadoop.shim.api.hbase.mapping;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Created by bryan on 2/2/16.
 */
public class ComparisonTypeTest {
  @Test
  public void testGetAllOperators() {
    Set<String> allOperators = new HashSet<>( Arrays.asList( ColumnFilter.ComparisonType.getAllOperators() ) );
    ColumnFilter.ComparisonType[] comparisonTypes = ColumnFilter.ComparisonType.values();
    assertEquals( allOperators.size(), comparisonTypes.length );
    for ( ColumnFilter.ComparisonType comparisonType : comparisonTypes ) {
      assertTrue( allOperators.contains( comparisonType.toString() ) );
    }
  }

  @Test
  public void testGetStringOperators() {
    Set<String> stringOperators = new HashSet<>( Arrays.asList( ColumnFilter.ComparisonType.getStringOperators() ) );
    ColumnFilter.ComparisonType[] comparisonTypes = ColumnFilter.ComparisonType.values();
    List<ColumnFilter.ComparisonType> matchingComparisonTypes = new ArrayList<>();
    for ( ColumnFilter.ComparisonType comparisonType : comparisonTypes ) {
      if ( comparisonType.toString().matches( "[A-Za-z ]+" ) ) {
        matchingComparisonTypes.add( comparisonType );
      }
    }
    assertEquals( matchingComparisonTypes.size(), stringOperators.size() );
    for ( ColumnFilter.ComparisonType matchingComparisonType : matchingComparisonTypes ) {
      assertTrue( stringOperators.contains( matchingComparisonType.toString() ) );
    }
  }

  @Test
  public void testGetNumericOperators() {
    Set<String> numericOperators = new HashSet<>( Arrays.asList( ColumnFilter.ComparisonType.getNumericOperators() ) );
    ColumnFilter.ComparisonType[] comparisonTypes = ColumnFilter.ComparisonType.values();
    List<ColumnFilter.ComparisonType> matchingComparisonTypes = new ArrayList<>();
    for ( ColumnFilter.ComparisonType comparisonType : comparisonTypes ) {
      if ( !comparisonType.toString().matches( "[A-Za-z ]+" ) ) {
        matchingComparisonTypes.add( comparisonType );
      }
    }
    assertEquals( matchingComparisonTypes.size(), numericOperators.size() );
    for ( ColumnFilter.ComparisonType matchingComparisonType : matchingComparisonTypes ) {
      assertTrue( numericOperators.contains( matchingComparisonType.toString() ) );
    }
  }

  @Test
  public void testStringToOpp() {
    for ( ColumnFilter.ComparisonType comparisonType : ColumnFilter.ComparisonType.values() ) {
      assertEquals( comparisonType, ColumnFilter.ComparisonType.stringToOpp( comparisonType.toString() ) );
    }
    assertNull( ColumnFilter.ComparisonType.stringToOpp( "fake" ) );
  }

  @Test
  public void testValueOf() {
    for ( ColumnFilter.ComparisonType comparisonType : ColumnFilter.ComparisonType.values() ) {
      assertEquals( comparisonType, ColumnFilter.ComparisonType.valueOf( comparisonType.name() ) );
    }
  }
}

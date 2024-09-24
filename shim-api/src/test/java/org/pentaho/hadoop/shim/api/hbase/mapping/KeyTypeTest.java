/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package org.pentaho.hadoop.shim.api.hbase.mapping;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by bryan on 2/2/16.
 */
public class KeyTypeTest {
  @Test
  public void testKeyType() {
    for ( Mapping.KeyType keyType : Mapping.KeyType.values() ) {
      assertEquals( keyType, Mapping.KeyType.valueOf( keyType.name() ) );
      StringBuilder valbuilder = new StringBuilder();
      for ( String s : keyType.name().split( "_" ) ) {
        valbuilder.append( s.substring( 0, 1 ) );
        valbuilder.append( s.substring( 1 ).toLowerCase() );
      }
      assertEquals( valbuilder.toString(), keyType.toString() );
    }
  }
}

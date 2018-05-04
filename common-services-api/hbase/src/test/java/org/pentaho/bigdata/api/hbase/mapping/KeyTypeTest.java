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

package org.pentaho.bigdata.api.hbase.mapping;

import org.junit.Test;
import org.pentaho.bigdata.api.hbase.mapping.Mapping;

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
